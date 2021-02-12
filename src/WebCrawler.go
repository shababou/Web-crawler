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
const STATUS = "status"
const RESULT = "result"

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
	NbWorkers int `json:"workers"`
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
	workersStatus *TeamWorkers
}

type Job struct {
	Def *JobDef
	ProcessData *JobProcessData
	Status *JobStatus
	Result *JobResult
}

type Jobs struct {
	jobs map[string]*Job
}





















/* Getting the status of of Team of workers.
This method shall return a slice for the workers' status of the receiver specified team, as well as the number of active workers.
*/
func (team *TeamWorkers) GetTeamStatus() ([]bool, int) {
	nbActive := 0
	status := make([]bool, len(team.workers))
    for i, w := range team.workers {
        if w.status {
            nbActive++
            status[i] = true
        }
    }
    return status, nbActive
}

/* Updating job summary.
This method shall update the status and result parameters of the receiver specified job:
- the status shall consist in the number of in_progress URLs and completed URLs among the URLs defined in this job,
- the result shall consist in the list of unique all data retrieved from the crawling for each of the URLs defined in the job.
*/
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
      		(*job.Result)[url] = completedData
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


/* Job worker in action.
This method shall define the work cycle of the worker specified by its id as following:
1- The worker shall iterate through the URLs defined in the specified receiver job and select the first child URL available in the URLs waiting list.
2- If a child URL is available, the worker shall:  
	- update its status to true meaning he is active,
	- remove the selected URL child from the URLs waiting list,
	- crawl the selected URL,
	- add the selected URL child to the URLs completed list.
   Else, the worker shall:
	- update its status to false meaning he is inactive,
	- get the activity status of the other job team workers and:
		- iterate through step 1- until finding a task if at least one worker of the team is active,
		- leave the job else.
	  Rationale: the worker leaves the job only if all the other workers are inactive, meaning that the job is completed.
*/
func (job *Job) WorkOnJob(workerId int, wgJob *sync.WaitGroup) {
	// Giving a name to the worker for traces
	workerName := "Job_" + job.Def.Job_id + "_worker" + strconv.Itoa(workerId)

	// Setting the worker' status to true at the beginning of its work
	nbActiveWorkers := job.Def.NbWorkers
	jobWorkers := job.ProcessData.workersStatus
	jobWorkers.workers[workerId].status = true

	jobProcessData := job.ProcessData

	// Working...
	WorkingLoop: 
	for{

		// For each URL defined in the specified receiver job...
		for url, _ := range jobProcessData.waitingUrls {
			urlWaitingData := jobProcessData.waitingUrls[url]
			// Looking for a child URL waiting to be crawled
			urlWaitingData.Lock()
			fmt.Println(workerName + " looking for a task in URL: " + url + "\n")
	        if(len(urlWaitingData.ChildrenUrls) > 0){
	        	// At least one child URL ready to be crawled
	        	// Getting the first available child URL to crawl 
	        	for urlChild, _ := range urlWaitingData.ChildrenUrls {
	        		fmt.Println(workerName + " getting ready to crawl URL: " + urlChild + "\n")
	        		// Updating worker' status to true
			        jobWorkers.workers[workerId].status = true
	        		// Removing the child URL selected from the waiting URLs since ready to be crawled by the worker
    				delete(urlWaitingData.ChildrenUrls, urlChild)
			        urlWaitingData.Unlock()

			        // Performing crawling task
			        fmt.Println(workerName + " crawling URL: " + urlChild + " ...\n")
			        urlWaitingData.CrawlUrl(&urlChild, jobProcessData.completedUrls[url])

			        // Ending crawling task
			        fmt.Println(workerName + " completed crawling URL: " + urlChild + "\n")
			        // Adding the crawled URL to the completed URLs
			        jobProcessData.completedUrls[url].Lock()
			        jobProcessData.completedUrls[url].Urls[urlChild] = urlWaitingData.Data
			        jobProcessData.completedUrls[url].Unlock()
			        fmt.Println(workerName + " finished task on URL: " + urlChild + "\n")

			        // Looking back for a new task...
			        goto WorkingLoop
		        	
		        }
	        }
	        urlWaitingData.Unlock()
	    }

	    // Updating worker' status to false, as inactive because no task available
	    jobWorkers.workers[workerId].status = false
	    
	    // Checking the activity of other job team workers
	    jobWorkers.Lock()
	    _, nbActiveWorkers = jobWorkers.GetTeamStatus()
	    jobWorkers.Unlock()
	    if(nbActiveWorkers == 0){
	    	// Leaving the job since all job team workers inactive
	    	break WorkingLoop
	    }
	    
	}

	fmt.Println(workerName + " leaving Job_" + job.Def.Job_id + "\n")
    wgJob.Done()
}

/* Job processing.
This method shall process the specified receiver job by launching as many goroutines as the workers number defined in the
specified receiver job.
The job shall be processing until all the workers have ended their work on this job.
*/
func (job *Job) ProcessJob() {
	var wgJob sync.WaitGroup

	// Launching the goroutines workers to work on the job
	nbWorkers := job.Def.NbWorkers
	wgJob.Add(nbWorkers)
	for i := 0; i < nbWorkers; i++ {
		go job.WorkOnJob(i, &wgJob)
	}
	
	// Waiting until the work on job is completed.
	wgJob.Wait()
	fmt.Println("Job_ " + job.Def.Job_id + " completed !")
}


/* Job initialization.
This method shall initialize a job of Job type by:
- assigning the specified receiver JobDef to the Def parameter,
- creating the ProcessData parameter by:
	- initializing the waitingUrls parameter by initializing the UrlData for each URLs provided by the specified JobDef,
	- initializing the completedUrls parameter by initializing the mapping between each URLs provided by the specified JobDef and their
	list of children URLs with their associated data.
- creating the JobStatus and Result parameters
Note 1: at this init step, for waitingUrls, UrlData for each specified URL shall contain only one childrenUrls equal to the specified URL with empty associated data.
Note 2: at this init step, for completedUrls, the list of children URLs for each specified URL shall be empty as well as the associated data.
*/
func (job *Job) InitJob(jobDef *JobDef) {
	// Assigning Def parameter
	job.Def = jobDef

	// Creating ProcessData parameter
	jobProcessData := &JobProcessData{}
    teamWorkers := make([]Worker, job.Def.NbWorkers)
	job.ProcessData = jobProcessData
	jobProcessData.workersStatus = &TeamWorkers{workers:teamWorkers} 
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

    // Initializing JobStatus and Result parameters of Job
    job.Status = &JobStatus{Completed:0 , InProgress:0} 
    job.Result = &JobResult{} 
}

/* Getting job status and result end point implementation.
This method shall read the content of an http request, and make sure that the HTTP request is composed by one of the following synthaxis:
1- /jobs/{job_id}/status,
2- /jobs/{job_id}/result.
If request is one of the cases 1- and 2- above, and if job_id is existing among allJobs specified receiver parameter, code 200 shall be displayed, and following shall be performed:
- updating the specified job status
- display the response as a new JSON of JobStatus of JobResult type, respectively if request content is "status" or "result".
Else, code 404 shall be caught and displayed. 
*/
func (allJobs *Jobs) GetJobData(w http.ResponseWriter, r *http.Request) {
	urlJobIdPos := 2
	urlServiceNamePos := 3

	// Reading and splitting the URL given in the request
	urlParts := strings.Split(r.URL.String(), "/")

	// Checking if HTTP request is success: code 200 
	if  ( (len(urlParts) == 4) && ((urlParts[urlServiceNamePos] == STATUS)||(urlParts[urlServiceNamePos] == RESULT)) ) {
		jobId := urlParts[urlJobIdPos]
		job, existing := allJobs.jobs[jobId]
		if(existing) {
			// Updating the status of the specified job, necessary to display either status or result data response
			job.UpdateJobStatus()

			// Displaying the JSON response according to the request content 
			if  (urlParts[urlServiceNamePos] == STATUS) {
				WriteJson(w, job.Status)
			} else if (urlParts[urlServiceNamePos] == RESULT) {
		   		WriteJson(w, job.Result)
			}
		}
	// HTTP request incorrect: code 404
	} else {
		w.WriteHeader(http.StatusNotFound)
		return
	}
}

/* Adding job end point implementation.
This method shall read the content of an http request, and make sure the request is JSON content type.
The JSON decoded shall be JobDef structure type. 
If the request is not a JSON content type or malformed JSON, code 400 shall be caught and displayed.
Else, code 200 shall be caught and displayed, and following shall be performed:
- create a unique job_id value,
- make sure that there is at least one worker,
- display the response as a new JSON of JobDef type that shall be the same as the request one with the job_id added,
- initialize the new job as Job type with the parameters specified in the request,
- add this new job to the allJobs specified receiver,
- once all the above steps completed, start a goroutine to process the created job.
*/
func (allJobs *Jobs) AddJob(w http.ResponseWriter, r *http.Request) {
	jobDef := &JobDef{}

	// Reading the HTTP request content
	bytes, _ := ioutil.ReadAll(r.Body)

	// Decoding the JSON content: code 400 is incorrect 
	ct := r.Header.Get("content-type")
	err := json.Unmarshal(bytes, jobDef)
	if (err != nil) || (ct != "application/json") {
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte(err.Error()))
		return
	} 

	// Creating a unique job_id
	jobDef.Job_id = fmt.Sprintf("%d", time.Now().UnixNano())

	// Setting number or workers at least equal to 1
	if(jobDef.NbWorkers < 1){
		jobDef.NbWorkers = 1
	}

	// Displaying the JSON response with job_id defined, and code 200 if success
	WriteJson(w, jobDef)

	// Initializing the new job and adding it to allJobs
	newJob := &Job{}
	newJob.InitJob(jobDef)
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
