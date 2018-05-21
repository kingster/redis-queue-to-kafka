package relay

import (
	"fmt"
	"github.com/Comcast/go-leaderelection"
	"github.com/samuel/go-zookeeper/zk"
	"log"
	"time"
)

type ElectionResponse struct {
	IsLeader    bool
	CandidateID string
}

type LeaderElector struct {
	ZKConnect     []string
	ElectionNode  string
	resultChannel chan ElectionResponse
	connFailCh    chan bool
	leaderTask    func()
}

func (leadership *LeaderElector) SetTask(f func()) {
	leadership.leaderTask = f
}

func (leadership *LeaderElector) Elect() {

	// Create the ZK connection
	zkConn, _, err := zk.Connect(leadership.ZKConnect, 5*time.Second)
	if err != nil {
		log.Printf("Error in zk.Connect (%s): %v", leadership.ZKConnect, err)
		panic(err)
	}
	defer zkConn.Close()

	electionNode := leadership.ElectionNode

	// Create the election node in ZooKeeper
	exists, _, err := zkConn.Exists(electionNode)
	if err != nil {
		log.Printf("Erorr: Checking Election Node %v", electionNode, err)
		panic(err)
	}

	if !exists {
		_, err = zkConn.Create(electionNode, []byte(""), 0, zk.WorldACL(zk.PermAll))
		if err != nil {
			log.Printf("Error creating the election node (%s): %v", electionNode, err)
			panic(err)
		}
	}

	election, err := leaderelection.NewElection(zkConn, electionNode, hostname())
	go election.ElectLeader()

	leadership.resultChannel = make(chan ElectionResponse)
	leadership.connFailCh = make(chan bool)

	go leadership.logElectionResults()

	var status leaderelection.Status
	var ok bool
	for {
		select {
		case status, ok = <-election.Status():
			if !ok {
				fmt.Println("\t\t\tChannel closed, election is terminated!!!")
				leadership.resultChannel <- ElectionResponse{false, status.CandidateID}
				election.Resign()
				return
			}
			if status.Err != nil {
				fmt.Println("Received election status error <<", status.Err, ">> for candidate <", status.CandidateID, ">.")
				election.Resign()
				return
			}

			log.Println("Candidate received status message: <", status, ">.")
			if status.Role == leaderelection.Leader {
				leadership.doLeaderStuff(election, status, leadership.resultChannel, leadership.connFailCh)
				return
			}
		case <-leadership.connFailCh:
			fmt.Println("\t\t\tZK connection failed for candidate <", status.CandidateID, ">, exiting")
			leadership.resultChannel <- ElectionResponse{false, status.CandidateID}
			election.Resign()
			return
			//case <-time.After(time.Second * 100):
			//	fmt.Println("\t\t\tERROR!!! Timer expired, stop waiting to become leader for", status.CandidateID)
			//	election.Resign()
			//	leadership.resultChannel <- ElectionResponse{false, status.CandidateID}
			//	return
		}
	}
}

func (leadership *LeaderElector) logElectionResults() {
	for elem := range leadership.resultChannel {
		log.Println("LeaderShip Update :::: ", elem)
	}
}

func (leadership *LeaderElector) doLeaderStuff(leaderElector *leaderelection.Election, status leaderelection.Status, respCh chan ElectionResponse, connFailCh chan bool) {

	respCh <- ElectionResponse{true, status.CandidateID}
	//if !strings.EqualFold(status.NowFollowing, "") {
	//	fmt.Println("\t\t\tLeader", status.CandidateID, "is unexpectedly following <",
	//		status.NowFollowing, ">")
	//	panic("Test failed!!!! Please please please find a better way to signal test failure")
	//}
	log.Println("\tCandidate <", status.CandidateID, "> is Leader.")

	// do some work when I become leader
	select {
	case <-connFailCh:
		fmt.Println("\t\t\tZK connection failed while LEADING, candidate <", status.CandidateID, "> resigning.")
		leaderElector.Resign()
		return
	case status, ok := <-leaderElector.Status():
		if !ok {
			fmt.Println("\t\t\tChannel closed while Leading, candidate <", status.CandidateID, "> resigning.")
			leaderElector.Resign()
			return
		} else if status.Err != nil {
			fmt.Println("\t\t\tError detected in whil leading, candidate <", status.CandidateID, "> resigning. Error was <",
				status.Err, ">.")
			leaderElector.Resign()
			return
		} else {
			fmt.Println("\t\t\tUnexpected status notification while Leading, candidate <", status.CandidateID, "> resigning.")
			leaderElector.Resign()
			return
		}
	default:
		// does (almost) no work, uncommenting next block will simulate longer work
		//	case <-time.NewTimer(1 * time.Second).C:

		fmt.Println("Starting Task....")
		leadership.leaderTask()
	}

	fmt.Print("\tCandidate <", status.CandidateID, "> has completed its work, it is resigning.\n\n")
	//	leaderElector.EndElection()
	leaderElector.Resign()
	return
}
