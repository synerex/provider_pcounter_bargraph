package main

import (
	"bufio"
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/golang/protobuf/ptypes"
	"github.com/yosuke-furukawa/json5/encoding/json5"

	pGeo "github.com/synerex/proto_geography"
	pcounter "github.com/synerex/proto_pcounter"
	api "github.com/synerex/synerex_api"
	pbase "github.com/synerex/synerex_proto"
	sxutil "github.com/synerex/synerex_sxutil"
)

var (
	RAND_MAX = 32767
)

var (
	nodesrv           = flag.String("nodesrv", "127.0.0.1:9990", "Node ID Server")
	local             = flag.String("local", "", "If you want to connect local synerex server directory")
	bin               = flag.Int("bin", 300, "How many seconds to aggregate (default 300sec)")
	config            = flag.String("config", "config.json", "Config file (default config.json)")
	mu                sync.Mutex
	version           = "0.01"
	barCount          int
	pctCount          int
	barMap            map[string]*counter
	pccp              *pcConfig
	currentBinTime    time.Time
	nextBinTime       time.Time
	doubleNextBinTime time.Time
	supplyClient      *sxutil.SXServiceClient
	sxServerAddress   string
)

type counter struct {
	Name              string     `json:"name"`
	Devices           [][2]int   `json:"devices"` // device id, counter no. pairs.
	Lonlat            [2]float64 `json:"lonlat"`  // use first sendor data?
	CurrentBinForward int        `json:"currentBin,omitempty"`
	NextBinForward    int        `json:"nextBin,omitempty"`
	CurrentBinBack    int        `json:"currentBinBack,omitempty"`
	NextBinBack       int        `json:"nextBinBack,omitempty"`
}

type pcConfig struct {
	Devices   []string     `json:"devices"`
	Locations [][2]float64 `json:"locations"`
	Counters  []counter    `json:"counters"`
}

func readPcConfig() {
	var pcc pcConfig
	fp, err := os.Open(*config)
	if err != nil {
		fmt.Println("We need config.json file to convert.")
		panic(err)
	}
	defer fp.Close()

	reader := bufio.NewReaderSize(fp, 4096)
	dec := json5.NewDecoder(reader)
	jerr := dec.Decode(&pcc)
	if jerr != nil {
		fmt.Println("Json format error")
		fmt.Println(jerr.Error())
		os.Exit(1)
	}
	fmt.Printf("loaded %d devices, %d counters \n", len(pcc.Devices), len(pcc.Counters))
	//	fmt.Printf("%d %#v\n", len(pcc.Devices), pcc.Devices)
	//	fmt.Printf("%d %#v\n", len(pcc.Locations), pcc.Locations)

	// set global
	barCount = len(pcc.Counters)
	pccp = &pcc

	// "MAC address" -> *counter map
	barMap = make(map[string]*counter, len(pcc.Devices))

	for i := range pcc.Devices {
		v := &pcc.Devices[i]
		for j := range pcc.Counters {
			c := &pcc.Counters[j]
			for k := range c.Devices {
				if c.Devices[k][0] == i { // if device is in the counter,
					// we suppose only one device with one counter.
					barMap[*v+"-"+strconv.Itoa(c.Devices[k][1])] = c
				}
			}
		}
	}
}

// We need to check how many is allowed.

// just for stat debug
func monitorStatus() {
	for {
		sxutil.SetNodeStatus(int32(pctCount), "PC-Bar")
		time.Sleep(time.Second * 3)
	}
}

func sendCurrentBins() {
	clt := supplyClient
	//	fmt.Printf("Time: %v\n", sim.GetGlobalTime())
	bars := make([]*pGeo.BarGraph, 0, barCount)
    tss, _ := ptypes.TimestampProto(currentBinTime)
    smf := 0
    smb := 0    
	for i,_ := range pccp.Counters {
        ct := &pccp.Counters[i]
        mu.Lock()
		bars = append(bars, &pGeo.BarGraph{
			Id:     int32(i),
			Ts:     tss,
			Type:   pGeo.BarType_BT_HEX_FIXCOLOR,
			Color:  0x00FF00,
			Lon:    ct.Lonlat[0],
			Lat:    ct.Lonlat[1],
			Width:  2,
			Radius: 4,
			BarData: []*pGeo.BarData{
				&pGeo.BarData{
					Value: float64(ct.CurrentBinForward),
					Label: "Forward",
					Color: 0x69d382,
				},
				&pGeo.BarData{
					Value: float64(ct.CurrentBinBack),
					Label: "Back",
					Color: 0x4080af,
				},
			},
			Min:  1,
			Max:  5000,
			Text: ct.Name,
		})
        smf += ct.CurrentBinForward
        smb += ct.CurrentBinBack
		// shift
		ct.CurrentBinForward = ct.NextBinForward
		ct.CurrentBinBack = ct.NextBinBack
		ct.NextBinForward = 0
		ct.NextBinBack = 0
        mu.Unlock()        
	}

	barList := pGeo.BarGraphs{
		Bars: bars,
	}
	out, _ := proto.Marshal(&barList)
	cont := api.Content{Entity: out}
	smo := sxutil.SupplyOpts{
		Name:  "BarGraphs",
		Cdata: &cont,
	}
	_, nerr := clt.NotifySupply(&smo)
	if nerr != nil { // connection failuer with current client
		log.Printf("Connection failure", nerr)
	} else {
		log.Printf("Send Bar! %s %d %d", ptypes.TimestampString(tss), smf, smb )
	}
}

func supplyPCounterCallback(clt *sxutil.SXServiceClient, sp *api.Supply) {

	pc := &pcounter.PCounter{}
	err := proto.Unmarshal(sp.Cdata.Entity, pc)
	if err == nil { // get Pcounter
		//		log.Printf("Got %v", pc)
		ts0, _ := ptypes.Timestamp(pc.Ts)
		if currentBinTime.IsZero() { // need to initialize bins.
			//			sec := ts0.Min()*60 +ts0.Second()
			//			sec = sec - (sec%*bin)  // just make it for each bin.
			//			crurentBinTime　= Date(ts0.Year(), ts0.Month(), ts0.Day(), ts0.Hour(), int(sec/60), sec%60, time.Local)
			// truncate into Bin time.
            mu.Lock()
			currentBinTime = ts0.Truncate(time.Duration(*bin) * time.Second)
			nextBinTime = currentBinTime.Add(time.Duration(*bin) * time.Second)
			doubleNextBinTime = currentBinTime.Add(time.Duration(*bin*2) * time.Second)
            mu.Unlock()            
		} else if ts0.After(doubleNextBinTime) { // send current Bin data.
			sendCurrentBins()
            mu.Lock()            
			currentBinTime = nextBinTime
			nextBinTime = doubleNextBinTime
			doubleNextBinTime = nextBinTime.Add(time.Duration(*bin) * time.Second)
            mu.Unlock()                        
		}

		//		ld := fmt.Sprintf("%s,%s,%s,%s,%s",ts0,pc.Hostname,pc.Mac,pc.Ip,pc.IpVpn)
		//		ds.store(ld)
		for _, ev := range pc.Data {
			//			line := fmt.Sprintf("%s,%s,%d,%s,%s,",ts,pc.DeviceId,ev.Seq,ev.Typ,ev.Id)
			switch ev.Typ {
			case "counter":
				cx, ok := barMap[pc.DeviceId+"-"+ev.Id] // should be same as deviceAggName
				if ok {
					pctCount++
                    mu.Lock()                                                        
					if ev.Dir == "f" {

						cx.CurrentBinForward++
					} else if ev.Dir == "b" {
						cx.CurrentBinBack++
					} else {
						log.Printf(":Unkown %s:  %s %s %s", ptypes.TimestampString(ev.Ts), pc.DeviceId, ev.Id, ev.Dir)
					}
                    mu.Unlock()                                                        
				} else {
					//					log.Printf("Can't find counter Device [%s] in config.json", pc.DeviceId+"-"+ev.Id)
				}
				//				line = line + fmt.Sprintf("%s,%d",ev.Dir,ev.Height)
			case "fillLevel":
				//				line = line + fmt.Sprintf("%d",ev.FillLevel)
			case "dwellTime":
				//				tsex := ptypes.TimestampString(ev.TsExit)
				//				line = line + fmt.Sprintf("%f,%f,%s,%d,%d",ev.DwellTime,ev.ExpDwellTime,tsex,ev.ObjectId,ev.Height)
			}
			//			ds.store(line)
		}
		//		*/
	}
}

func subscribePCounterSupply(client *sxutil.SXServiceClient) {
	ctx := context.Background() //
	client.SubscribeSupply(ctx, supplyPCounterCallback)
	log.Fatal("Error on subscribe")
}

func main() {
	flag.Parse()
	go sxutil.HandleSigInt()
	wg := sync.WaitGroup{} // for syncing other goroutines

	readPcConfig() // read pcConfig

	//	log.Printf("Map %#v", barMap)

	name := "PC_Bar"
	sxutil.RegisterDeferFunction(sxutil.UnRegisterNode)
	channelTypes := []uint32{pbase.PEOPLE_COUNTER_SVC, pbase.GEOGRAPHIC_SVC}
	srv, rerr := sxutil.RegisterNode(*nodesrv, name, channelTypes, nil)

	if rerr != nil {
		log.Fatal("Can't register node ", rerr)
	}

	if *local != "" {
		srv = *local // use local server address
	}

	log.Printf("Connecting SynerexServer at [%s]\n", srv)

	client := sxutil.GrpcConnectServer(srv)
	sxServerAddress = srv
	argJSON := fmt.Sprintf(name)

	supplyClient = sxutil.NewSXServiceClient(client, pbase.GEOGRAPHIC_SVC, argJSON)

	subscribeClient := sxutil.NewSXServiceClient(client, pbase.PEOPLE_COUNTER_SVC, argJSON)

	wg.Add(1)
	go monitorStatus() // keep status

	log.Printf("Starting PCounter _ Barcounter converter")

	go subscribePCounterSupply(subscribeClient)

	wg.Wait()
}
