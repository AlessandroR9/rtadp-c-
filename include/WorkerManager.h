#ifndef WORKERMANAGER_H
#define WORKERMANAGER_H
 
// Copyright (C) 2024 INAF
// This software is distributed under the terms of the BSD-3-Clause license
//
// Authors:
//
//    Andrea Bulgarelli <andrea.bulgarelli@inaf.it>
//
#include <iostream>
#include <thread>
#include <vector>
#include <queue>
#include <mutex>
#include <condition_variable>
#include <atomic>
#include <chrono>
#include <zmq.hpp>
#include "MonitoringPoint.h"
#include "WorkerThread.h"
#include "MonitoringThread.h"
#include "Supervisor.h"
#include "WorkerProcess.h"


using json = nlohmann::json;
class WorkerProcess;
class WorkerThread;
 
class WorkerManager {


private:
    int manager_id;
    Supervisor* supervisor;
    std::string name;
    std::string status;
    std::string workersname;
    std::shared_ptr<json> config;
    WorkerLogger* logger;
    std::string fullname;
    std::string globalname;
    std::string processingtype;
    int max_workers;
    std::string result_socket_type;
    std::string result_lp_socket;
    std::string result_hp_socket;
    std::string result_dataflow_type;
    std::vector<zmq::socket_t*> socket_lp_result;
    std::vector<zmq::socket_t*> socket_hp_result;
    int pid;
    zmq::context_t&  context;
    zmq::socket_t* socket_monitoring;
    std::shared_ptr<std::queue<std::string>> low_priority_queue;
    std::shared_ptr<std::queue<std::string>> high_priority_queue;
    std::shared_ptr<std::queue<std::string>> result_lp_queue;
    std::shared_ptr<std::queue<std::string>> result_hp_queue;
    MonitoringPoint* monitoringpoint;
    MonitoringThread* monitoringthread;
    std::vector<std::shared_ptr<WorkerThread>> workerprocesses;
    std::vector<std::shared_ptr<WorkerProcess>> worker_processes;
    std::thread worker_thread;  // Thread object for running the manager
    int num_workers;
    int workersstatus;
    int workersstatusinit;
    std::shared_ptr<std::mutex> tokenresultslock;
    std::shared_ptr<std::mutex> tokenreadinglock;
    std::atomic<bool> continueall;
    std::atomic<int> processdata;
    std::atomic<bool> stopdata;
    std::atomic<bool> _stop_event;
    std::atomic<int> processdata_shared;
    std::vector<std::atomic<int>> worker_status_shared;
    std::vector<std::atomic<double>> processing_rates_shared;
    std::vector<std::atomic<int>> total_processed_data_count_shared;
 
    // Helper function to clean a single queue
    void clean_single_queue(std::shared_ptr<std::queue<std::string>>& queue, const std::string& queue_name);
 
    // Helper function to close a queue
    void close_queue(std::shared_ptr<std::queue<std::string>>& queue, const std::string& queue_name);

public:
    // Constructor
    WorkerManager(int manager_id, Supervisor* supervisor, const std::string& name = "None");
 
    // Function to change token results
    void change_token_results();
 
    // Function to change token reading
    void change_token_reading();
 
    // Function to set stop data flag
    void set_stopdata(bool stopdata);
 
    // Function to set process data flag
    void set_processdata(int processdata);
 
    // Function to change the status based on flags
    void change_status();
 
    // Function to start service threads
    void start_service_threads();
 
    // Function to start worker threads (to be reimplemented)
    virtual void start_worker_threads(int num_threads);
 
    // Function to start worker processes (to be reimplemented)
    virtual void start_worker_processes(int num_processes);
 
    // Main run function
    void run();

    void start();
 
    // Function to clean the queues
    void clean_queue();
 
    // Function to stop the manager
    void stop(bool fast = false);
 
    // Function to stop internal threads
    void stop_internalthreads();
 
    // Function to configure workers
    void configworkers(const json& configuration);

    void setWorkerStatus(int worker_id, int status);

    void setProcessingRate(int worker_id, double rate);

    void setTotalProcessedDataCount(int worker_id, int count);

    // Public getter for supervisor
    Supervisor* getSupervisor() const;

    std::string getName() const;
    std::shared_ptr<std::queue<std::string>> getLowPriorityQueue() const;
    std::shared_ptr<std::queue<std::string>> getHighPriorityQueue() const;
    // std::shared_ptr<std::queue<json>> getResultLpQueue() const;
    // std::shared_ptr<std::queue<json>> getResultHpQueue() const;
    // std::shared_ptr<std::queue<json>> getLowPriorityQueue() const;
    // std::shared_ptr<std::queue<json>> getHighPriorityQueue() const;
    MonitoringPoint* getMonitoringPoint() const;
    MonitoringThread* getMonitoringThread() const;
    std::thread monitoring_thread;
    std::shared_ptr<std::queue<std::string>> getResultLpQueue() const;
    std::shared_ptr<std::queue<std::string>> getResultHpQueue() const;
 
    // Getters for result sockets
    std::string get_result_lp_socket() const { return result_lp_socket; }
    std::string get_result_hp_socket() const { return result_hp_socket; }
    std::string get_result_socket_type() const { return result_socket_type; }
    std::string get_result_dataflow_type() const { return result_dataflow_type; }
    std::string get_globalname() const { return globalname; }
    std::string getFullname() const;

    // Getter for status
    std::string getStatus() const;

    std::vector<std::shared_ptr<WorkerThread>> worker_threads;
    std::vector<std::thread> worker_threads_run;  // Vettore per i thread


    // Getter for stopdata
    bool getStopData() const;

    int getWorkersStatusInit() const;
    
    int getProcessDataSharedValue() const;

    // Getter for workersstatus
    int getWorkersStatus() const;

    // Getter for workersname
    std::string getWorkersName() const;

    // Getter for processingtype
    std::string getProcessingType() const;

    // Getter for worker_processes
    std::vector<std::shared_ptr<WorkerThread>> getWorkerProcesses();

    std::vector<std::shared_ptr<WorkerProcess>> getWorker_Processes();

    // Getter for processing_rates_shared
    std::vector<std::atomic<double>>& getProcessingRatesShared();

    // Getter for total_processed_data_count_shared
    std::vector<std::atomic<int>>& getTotalProcessedDataCountShared();
    
    // Getter for worker_status_shared
    std::vector<std::atomic<int>>& getWorkerStatusShared();

    // Getter for worker_threads
    std::vector<std::shared_ptr<WorkerThread>> getWorkerThreads();


};
 
#endif // WORKERMANAGER_H#
