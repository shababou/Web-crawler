package main

import (
	"fmt"
	"strings"
	"sync"
	"time"
	"encoding/json"
	"io/ioutil"
	"net/http"
	"strconv"
	. "UrlCrawling"
	. "Utilities"
)

const PORT = ":8080"

type Worker struct {
	status bool
}

type TeamWorkers struct {
	sync.Mutex
	workers []Worker
}

type JobDef struct {
	Job_id string `json:"job_id"`
	Urls []string `json:"urls"`
	Workers int `json:"workers"`
}
type JobStatus struct {
	sync.Mutex
	Completed int `json:"completed"`
	InProgress int `json:"in_progress"`
}
type JobResult map[string][]string 



type JobProcessData struct {
	waitingUrls map[string]*UrlData
	completedUrls map[string]*MapUrls
	workersStatus TeamWorkers
}

type Job struct {
	Def JobDef
	ProcessData JobProcessData
	Status JobStatus
	Result JobResult
}

type Jobs struct {
	jobs map[string]*Job
}



















func (allJobs *Jobs) GetJobData(w http.ResponseWriter, r *http.Request) {
	urlParts := strings.Split(r.URL.String(), "/")

	jobId := urlParts[2]

	job, existing := allJobs.jobs[jobId]


	if  ( ((urlParts[3] != "status")&&(urlParts[3] != "result")) || !existing) {
		w.WriteHeader(http.StatusNotFound)
		return
	}


job.UpdateJobStatus()

	if  (urlParts[3] == "status") {
	
		WriteJson(w, job.Status)

		

	} else if (urlParts[3] == "result") {


   		WriteJson(w, job.Result)
	}

}

func (job *Job) UpdateJobStatus() {
	/*
			completedUrls := []string{}
		jobCompletedUrls := job.ProcessData.completedUrls
		for url, urlData := range jobCompletedUrls {
			urlData.Lock()
			
			completedData := []string{}
			for childUrl, childUrlData := range urlData.urls {
				completedUrls = append(completedUrls, childUrl) 
	        	completedData = append(completedData, childUrlData...) 

	      	} 
	      	completedUrls = RemoveSliceDuplicates(completedUrls)
      		completedData = RemoveSliceDuplicates(completedData)
      		job.Result[url] = completedData
      		urlData.Unlock()
      	}
      	job.Status.Completed = len(completedUrls)

      	jobWaitingUrls := job.ProcessData.waitingUrls
      	waitingUrls := []string{}
		for _, urlData := range jobWaitingUrls {
			urlData.Lock()
			for childUrl, _ := range urlData.childrenUrls {
				waitingUrls = append(waitingUrls, childUrl) 
	      	} 
	      	waitingUrls = RemoveSliceDuplicates(waitingUrls)
      		urlData.Unlock()
      	}
      	job.Status.InProgress = len(waitingUrls)
	*/
		jobCompletedUrls := job.ProcessData.completedUrls
		for url, UrlData := range jobCompletedUrls {
			UrlData.Lock()
			completedData := []string{}
			for _, childUrlData := range UrlData.Urls {
	        	completedData = append(completedData, childUrlData...) 

	      	} 
      		completedData = RemoveSliceDuplicates(completedData)
      		job.Result[url] = completedData
      		UrlData.Unlock()
      	}

      	completed := 0
      	inProgress := 0
      	jobWaitingUrls := job.ProcessData.waitingUrls
		for _, UrlData := range jobWaitingUrls {
			UrlData.Lock()
			if(len(UrlData.ChildrenUrls)==0){
				completed++
			} else {
				inProgress++
			}
      		UrlData.Unlock()
      	}
      	job.Status.Completed = completed
      	job.Status.InProgress = inProgress
 }

func (team *TeamWorkers) GetTeamStatus() int {
	nbActive := 0
	fmt.Println(len(team.workers))
    for _, s := range team.workers {
        if s.status {
            nbActive++
        }
    }
    return nbActive
}

func (job *Job) WorkOnJob(workerId int, wgJob *sync.WaitGroup) {
	workerName := "Job_" + job.Def.Job_id + "_worker" + strconv.Itoa(workerId)
	fmt.Println(workerName)
	job.ProcessData.workersStatus.workers[workerId].status = true
	nbActiveWorkers := len(job.ProcessData.workersStatus.workers)
	LoopWhileWaitingUrls: 
	for{

		for url, _ := range job.ProcessData.waitingUrls {
			urlWaitingData := job.ProcessData.waitingUrls[url]
			urlWaitingData.Lock()
			fmt.Println(workerName + " : locked to find a task in URL: " + url + "\n")
	        if(len(urlWaitingData.ChildrenUrls) > 0){
	        	//LoopOnChildrenUrls: 
	        	for urlChild, _ := range urlWaitingData.ChildrenUrls {
	        		
    				delete(urlWaitingData.ChildrenUrls, urlChild)
			        fmt.Println(workerName + ": URL: " + urlChild + " deleted \n")
			        urlWaitingData.Unlock()
			        fmt.Println(workerName + ": unlocked because found a task URL: " + urlChild + "\n")


			        crawled := make(chan bool)
			        job.ProcessData.workersStatus.workers[workerId].status = true

			        fmt.Println(workerName + ": URL: " + urlChild + " started ! \n")
			        go urlWaitingData.CrawlUrl(&urlChild, job.ProcessData.completedUrls[url], crawled)
			        <- crawled
			        

			        fmt.Println(workerName + " locked because completed URL: " + urlChild + " \n")
			        job.ProcessData.completedUrls[url].Lock()
			        (*(job.ProcessData.completedUrls[url])).Urls[urlChild] = urlWaitingData.Data
			        job.ProcessData.completedUrls[url].Unlock()
			        fmt.Println(workerName + " unlocked after completed URL: " + urlChild + " \n")

			        goto LoopWhileWaitingUrls
		        	
		        }
	        }
	        urlWaitingData.Unlock()
	        //fmt.Println(*workerName + " unlocked because did not find a task for URL: " + url + "\n")
	    }

	    job.ProcessData.workersStatus.workers[workerId].status  = false
	    
	    job.ProcessData.workersStatus.Lock()
	    nbActiveWorkers = job.ProcessData.workersStatus.GetTeamStatus()
	    job.ProcessData.workersStatus.Unlock()
	    if(nbActiveWorkers == 0){
	    	break LoopWhileWaitingUrls
	    }
	    
	}

	fmt.Println(workerName + " leaving Job_" + job.Def.Job_id + "\n")
    wgJob.Done()
}

/* Processing a job.
This method shall process the specified receiver job a job by launching as many goroutines as the workers number defined in the
specified receiver job.
The job shall be processing until all the workers have ended their work on this job.
*/
func (job *Job) ProcessJob() {
	var wgJob sync.WaitGroup

	// Launching the goroutines workers to work on the job
	nbWorkers := job.Def.Workers
	wgJob.Add(nbWorkers)
	for i := 0; i < nbWorkers; i++ {
		go job.WorkOnJob(i, &wgJob)
	}
	
	// Waiting until the work on job is completed.
	wgJob.Wait()
	fmt.Println("Job: " + job.Def.Job_id + " completed !")
}


/* Job initialization.
This method shall initialize a job of Job type by:
- assigning the specified receiver JobDef to the Def parameter,
- creating the ProcessData parameter by:
	- initializing the waitingUrls parameter by initializing the UrlData for each URLs provided by the specified JobDef,
	- initializing the completedUrls parameter by initializing the mapping between each URLs provided by the specified JobDef and their
	list of children URLs with their associated data.
Note 1: at this init step, for waitingUrls, UrlData for each specified URL shall contain only one childrenUrls equal to the specified URL with empty associated data.
Note 2: at this init step, for completedUrls, the list of children URLs for each specified URL shall be empty as well as the associated data.
*/
func (job *Job) InitJob(jobDef *JobDef) {
	// Assigning Def parameter
	job.Def = *jobDef

	// Creating ProcessData parameter
	jobProcessData := &job.ProcessData
	var teamWorkers = make([]Worker, job.Def.Workers)
	jobProcessData.workersStatus = TeamWorkers{workers:teamWorkers} 
	jobProcessData.waitingUrls = make(map[string]*UrlData)
	jobProcessData.completedUrls = make(map[string]*MapUrls)
	// For each specified URLs, initializing the UrlData for the waitingUrls parameter, and the completedUrls
	urlsDef := job.Def.Urls
	for _, url := range urlsDef {
		// Initializing the waitingUrls for the current specified url
 		urlData := &UrlData{}
 		urlData.InitUrlData(&url)
    	jobProcessData.waitingUrls[url] = urlData
    	// Initializing the completedUrls for the current specified url
    	jobProcessData.completedUrls[url] = &MapUrls{Urls:map[string][]string{url:[]string{}}}
    }

    job.Status = JobStatus{Completed:0 , InProgress:0} 
    job.Result = JobResult{} 
}

/* Adding job end point implementation.
This method shall read the content of an http request, and make sure the request is JSON content type.
The JSON decoded shall be JobDef structure type. 
If request is not a JSON content type or malformed JSON, code 400 shall be caught and displayed.
Else, code 200 shall be caught and displayed, and following shall be performed:
- create a unique job_id value,
- display the response as a new JSON of JobDef type that shall be the same as the request one with the job_id added,
- initialize the new job as Job type with the parameters specified in the request,
- add this new job to the allJobs specified receiver,
- once all the above steps completed, start a goroutine to process the created job.
*/
func (allJobs *Jobs) AddJob(w http.ResponseWriter, r *http.Request) {
	jobDef := JobDef{}

	// Reading the HTTP request content
	bytes, _ := ioutil.ReadAll(r.Body)

	// Decoding the JSON content, and 
	ct := r.Header.Get("content-type")
	err := json.Unmarshal(bytes, &jobDef)
	if (err != nil) || (ct != "application/json") {
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte(err.Error()))
		return
	} 

	// Creating a unique job_id
	jobDef.Job_id = fmt.Sprintf("%d", time.Now().UnixNano())

	// Displaying the JSON response with job_id defined
	WriteJson(w, jobDef)

	// Initializing the new job and adding it to allJobs
	newJob := &Job{}
	newJob.InitJob(&jobDef)
	allJobs.jobs[jobDef.Job_id] = newJob

	// Starting the goroutine to process the job
	ready := true
	if(ready){
		go newJob.ProcessJob()	
	}

}




func main() {

	allJobs := Jobs{ jobs : map[string]*Job{} }

	// Adding job end point
	http.HandleFunc("/jobs", allJobs.AddJob)
	// Getting job status and result end point
	http.HandleFunc("/jobs/", allJobs.GetJobData)

	// Opening URL connection on http://localhost:PORT
    http.ListenAndServe(PORT, nil)

}
