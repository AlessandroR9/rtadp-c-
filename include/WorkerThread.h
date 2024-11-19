#ifndef WORKERTHREAD_H
#define WORKERTHREAD_H

#include <iostream>
#include <thread>
#include <queue>
#include <atomic>
#include <mutex>
#include <condition_variable>
#include <chrono>
#include <memory>
#include <string>
#include "json.hpp"
#include <zmq.hpp>
#include <spdlog/spdlog.h>
#include <spdlog/sinks/basic_file_sink.h>
#include "Supervisor.h"
#include "WorkerManager.h"
#include "WorkerBase.h"
#include "WorkerLogger.h"
#include "MonitoringPoint.h"

using json = nlohmann::json;

class WorkerThread {

    int worker_id;
    WorkerManager* manager;
    Supervisor* supervisor;
    WorkerBase* worker;
    std::string name;
    std::string workersname;
    std::string fullname;
    std::string globalname;
    WorkerLogger* logger;
    std::shared_ptr<std::queue<std::string>> low_priority_queue;
    std::shared_ptr<std::queue<std::string>> high_priority_queue;
    MonitoringPoint* monitoringpoint;

    std::chrono::time_point<std::chrono::high_resolution_clock> start_time;
    std::chrono::time_point<std::chrono::high_resolution_clock> next_time;
    int processed_data_count;
    int total_processed_data_count;
    double processing_rate;
    std::atomic<bool> _stop_event;
    std::atomic<int> processdata;
    std::atomic<int> status;
    int tokenresult;
    int tokenreading;
    std::unique_ptr<std::thread> timer;
    // std::thread worker_thread; 

    std::unique_ptr<std::thread> internal_thread;

    void start_timer(int interval);
    void workerop(int interval);
    void process_data(const std::string& data, int priority);


public:
    WorkerThread(int worker_id, WorkerManager* manager, const std::string& name, WorkerBase* worker);
    ~WorkerThread();

    void stop();
    void config(const json& configuration);
    void set_processdata(int processdata1);
    void run();

    std::unique_ptr<std::thread> get_internalthread();

    int get_tokenresult() const;
    void set_tokenresult(int value);
    int get_tokenreading() const;
    void set_tokenreading(int value);
    int get_status() const; 
    void set_status(int value); 

    int getWorkerId() const;

    double getProcessingRate() const;

    int getTotalProcessedDataCount() const;

    bool joinable() const;
    void join();

    
};

#endif // WORKERTHREAD_H
