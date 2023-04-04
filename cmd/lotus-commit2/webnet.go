package main

//func webget() {
//	inb, err := ioutil.ReadFile("32G.json")
//	if err != nil {
//		return
//	}
//
//	var c2in server_c2.Commit2In
//	if err := json.Unmarshal(inb, &c2in); err != nil {
//		return
//	}
//
//	var sp server_c2.SealerParam
//	sp.Sector.ID.Number = abi.SectorNumber(c2in.SectorNum)
//	sp.Sector.ID.Miner = abi.ActorID(111)
//	sp.Sector.ProofType = abi.RegisteredSealProof(c2in.SectorSize)
//	sp.Phase1Out = c2in.Phase1Out
//	var rp server_c2.Respones
//	if err = server_c2.RequestDo("/runcommit2", &sp, &rp); err != nil {
//		log.Println(err.Error())
//	}
//
//	fmt.Println("RequestDo:", rp.Code)
//	fmt.Printf("proof:%x", rp.Data.(string))
//}


