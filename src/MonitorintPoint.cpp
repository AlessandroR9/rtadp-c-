// Copyright (C) 2024 INAF
// This software is distributed under the terms of the BSD-3-Clause license
//
// Authors:
//
//    Andrea Bulgarelli <andrea.bulgarelli@inaf.it>
//
#include "MonitoringPoint.h"
#include "WorkerManager.h"

// Constructor to initialize the MonitoringPoint with a WorkerManager pointer
MonitoringPoint::MonitoringPoint(WorkerManager* manager)
    : manager(manager), processOS(getpid()) {
    data["header"]["type"] = 1;
    data["header"]["time"] = 0;  // Replace with actual timestamp if needed
    data["header"]["pidsource"] = manager->getFullname();
    data["header"]["pidtarget"] = "*";

    data["workermanagerstatus"] = "Initialised";  // Initial status
    data["procinfo"]["cpu_percent"] = 0;  // Initial CPU percent
    data["procinfo"]["memory_usage"] = 0;  // Initial memory usage
    data["queue_lp_size"] = 0;  // Initial low priority queue size
    data["queue_hp_size"] = 0;  // Initial high priority queue size

    std::cout << "MonitoringPoint initialised" << std::endl;
}

// Updates the data map with a new key-value pair
void MonitoringPoint::update(const std::string& key, const nlohmann::json& value) {
    //std::lock_guard<std::mutex> lock(data_mutex);
    data[key] = value;
}

// Retrieves the current data including system resource monitoring
nlohmann::json MonitoringPoint::get_data() {
    std::lock_guard<std::mutex> lock(data_mutex);
    resource_monitor();  // Update resource monitoring data
    data["header"]["time"] = std::time(0);  // Update timestamp
    set_status(manager->getStatus());  // Update status
    data["stopdatainput"] = manager->getStopData();  // Update stop data input

    // Update queue sizes
    update("queue_lp_size", manager->getLowPriorityQueue()->size());
    update("queue_hp_size", manager->getHighPriorityQueue()->size());
    update("queue_lp_result_size", manager->getResultLpQueue()->size());
    update("queue_hp_result_size", manager->getResultHpQueue()->size());

    // Update worker status
    update("workersstatusinit", manager->getWorkersStatusInit());
    update("workersstatus", manager->getWorkersStatus());
    update("workersname", manager->getWorkersName());

    if (manager->getProcessingType() == "thread") {
        for (const auto& worker : manager->getWorkerThreads()) {
            //WorkerThread* worker = worker_ptr.get();
            processing_rates[worker->getWorkerId()] = worker->getProcessingRate();
            processing_tot_events[worker->getWorkerId()] = worker->getTotalProcessedDataCount();
            worker_status[worker->getWorkerId()] = worker->get_status();
        }
    }

    // Update data with worker processing information
    data["worker_rates"] = processing_rates;
    data["worker_tot_events"] = processing_tot_events;
    data["worker_status"] = worker_status;
    return data;
}

// Sets the status in the data map
void MonitoringPoint::set_status(const std::string& new_status) {
    //std::lock_guard<std::mutex> lock(data_mutex);
    data["workermanagerstatus"] = new_status;
}

// Gets the current status from the data map
std::string MonitoringPoint::get_status() {
    //std::lock_guard<std::mutex> lock(data_mutex);
    return data["workermanagerstatus"].get<std::string>();
}

// Monitors and updates system resources (CPU, memory)
void MonitoringPoint::resource_monitor() {
    struct sysinfo memInfo;
    sysinfo(&memInfo);

    data["procinfo"]["cpu_percent"] = get_cpu_usage();
    data["procinfo"]["memory_usage"] = memInfo.totalram - memInfo.freeram;
}

// Placeholder function to get CPU usage
double MonitoringPoint::get_cpu_usage() {
    return 0.0;
}
