// Copyright (C) 2024 INAF
// This software is distributed under the terms of the BSD-3-Clause license
//
// Authors:
//
//    Andrea Bulgarelli <andrea.bulgarelli@inaf.it>
//
#include <memory>

#include <execinfo.h>
#include <unistd.h>

#include "WorkerThread.h"


using json = nlohmann::json;

//////////////////////////////////////////////////
WorkerThread::WorkerThread(int worker_id, WorkerManager* manager, const std::string& name, WorkerBase* worker)
    : worker_id(worker_id), manager(manager), name(name), worker(worker),
     processdata(0), status(0), tokenresult(worker_id), tokenreading(worker_id), _stop_event(false) {

    std::cout << "Creating a WorkerThread with name: " << name << std::endl;

    supervisor = manager->getSupervisor();
    workersname = supervisor->name + "-" + manager->getName() + "-" + name;
    fullname = workersname + "-" + std::to_string(worker_id);
    globalname = "WorkerThread-" + fullname;
    logger = supervisor->logger;

    worker->init(manager, supervisor, workersname, fullname);

    low_priority_queue = manager->getLowPriorityQueue();
    high_priority_queue = manager->getHighPriorityQueue();
    monitoringpoint = manager->getMonitoringPoint();

    start_time = std::chrono::high_resolution_clock::now();
    next_time = start_time;
    processed_data_count = 0;
    total_processed_data_count = 0;
    processing_rate = 0.0;


    spdlog::info("{} started", globalname);
    logger->system("WorkerThread started", globalname);

    internal_thread = std::make_unique<std::thread>(&WorkerThread::run, this);
}
//////////////////////////////////////////////////

void WorkerThread::config(const json& configuration) {
    worker->config(configuration);
}

void WorkerThread::set_processdata(int processdata1) {
    processdata = processdata1;
}

//////////////////////////////////////////////////
void WorkerThread::run() {
    start_timer(1);
    while (!_stop_event) {
        // std::this_thread::sleep_for(std::chrono::nanoseconds(10));
        if (processdata == 1 && tokenreading == 0) {
            // std::cout << "WORKERTHREAD RUN AAAAAAAAAAAAAAAAA" << std::endl;
            try {
                //std::cout << 'BBBBBBBBBBBBBBBBB' << std::endl;
                // Check and process high-priority queue first
                if (!high_priority_queue->empty()) {
                    auto high_priority_data = high_priority_queue->front();

                    // DEBUG
                    if (typeid(high_priority_data) == typeid(std::string)) {
                        spdlog::info("WorkerThread::run: high_priority_data is a string.");
                    }
                    else if (typeid(high_priority_data) == typeid(nlohmann::json)) {
                        spdlog::info("WorkerThread::run: high_priority_data is a JSON object.");
                    }
                    else {
                        spdlog::info("WorkerThread::run: high_priority_data is of unknown type: {}", typeid(high_priority_data).name());
                    }

                    high_priority_queue->pop();
                    manager->change_token_reading();
                    process_data(high_priority_data, 1);
                } else {
                    // Process low-priority queue if high-priority queue is empty
                    if (!low_priority_queue->empty()) {
                        auto low_priority_data = low_priority_queue->front();

                        // DEBUG
                        if (typeid(low_priority_data) == typeid(std::string)) {
                            spdlog::warn("WorkerThread::run: low_priority_data is a string.");
                        }
                        else if (typeid(low_priority_data) == typeid(nlohmann::json)) {
                            spdlog::info("WorkerThread::run: low_priority_data is a JSON object.");
                        }
                        else {
                            spdlog::info("WorkerThread::run: low_priority_data is of unknown type: {}", typeid(low_priority_data).name());
                        }

                        low_priority_queue->pop();
                        manager->change_token_reading();
                        process_data(low_priority_data, 0);
                    } else {
                        status = 2; // waiting for new data
                    }
                }
            } catch (const std::exception& e) {
                spdlog::warn("Exception caught in WorkerThread run: {}", e.what());

                // Rilancia l'eccezione dopo averla loggata
                // throw;
            }
        } else {
            if (tokenreading != 0 && status != 4) {
                status = 4; // waiting for reading from queue
            }
        }
    }

    if (internal_thread && internal_thread->joinable()) {
        internal_thread->detach();
    }
    spdlog::info("WorkerThread stop {}", globalname);
    logger->system("WorkerThread stop", globalname);
}
//////////////////////////////////////////////////

// Destructor
WorkerThread::~WorkerThread(){
    if (internal_thread && internal_thread->joinable()) {
        internal_thread->join();
    }
}

//////////////////////////////////////////////////
void WorkerThread::stop() {
    _stop_event = true;

    // Unisci tutti i thread prima di terminare
    if (internal_thread && internal_thread->joinable()) {
        internal_thread->join();
    }

    delete worker;
    status = 16; // stop
}
//////////////////////////////////////////////////

int WorkerThread::get_tokenresult() const {
    return tokenresult;
}

void WorkerThread::set_tokenresult(int value) {
    tokenresult = value;
}

int WorkerThread::get_tokenreading() const {
    return tokenreading;
}

void WorkerThread::set_tokenreading(int value) {
    tokenreading = value;
}

int WorkerThread::get_status() const { 
    return status;
}

int WorkerThread::getWorkerId() const {
    return worker_id;
}

double WorkerThread::getProcessingRate() const {
    return processing_rate;
}

int WorkerThread::getTotalProcessedDataCount() const {
    return total_processed_data_count;
}

void WorkerThread::set_status(int value) { 
    status = value;
}

bool WorkerThread::joinable() const {
    return internal_thread && internal_thread->joinable();
}

void WorkerThread::join() {
    if (internal_thread && internal_thread->joinable()) {
        internal_thread->join();
    }
}

// Function to start a timer
void WorkerThread::start_timer(int interval) {
    timer = std::make_unique<std::thread>(&WorkerThread::workerop, this, interval);
}

void WorkerThread::workerop(int interval) {
    while (!_stop_event) {
        // std::cout << "WORKEROPPPPPPPPPPPPPPPPPPP" << std::endl;
        std::this_thread::sleep_for(std::chrono::seconds(interval));

        auto elapsed_time = std::chrono::duration_cast<std::chrono::seconds>(std::chrono::high_resolution_clock::now() - next_time).count();
        next_time = std::chrono::high_resolution_clock::now();
        processing_rate = static_cast<double>(processed_data_count) / elapsed_time;
        total_processed_data_count += processed_data_count;
        spdlog::info("{} Rate Hz {:.1f} Current events {} Total events {} Queues {} {}", globalname, processing_rate, processed_data_count, total_processed_data_count, low_priority_queue->size(), high_priority_queue->size());
        logger->system(fmt::format("Rate Hz {:.1f} Current events {} Total events {} Queues {} {}", processing_rate, processed_data_count, total_processed_data_count, low_priority_queue->size(), high_priority_queue->size()), globalname);
        processed_data_count = 0;
    }
}

////////////////////////////////////////////
void WorkerThread::process_data(const std::string& data, int priority) {
    status = 8; // processing new data
    processed_data_count++;

    // spdlog::info("WorkerThread::process_data: Worker type: {}", typeid(*worker).name());
    spdlog::info("WorkerThread::process_data: Received data of size: {}", data.size());
    spdlog::info("WorkerThread::process_data: Called with priority: {}", priority);
    // spdlog::info("WorkerThread::process_data: DATA: {}", data);

    if (!worker) {
        spdlog::error("WorkerThread::process_data: worker is null");
        return;
    }

    auto dataresult = worker->processData(data, priority);

    if (!dataresult.empty() && tokenresult == 0) {
        spdlog::warn("DENTRO IL CONTROLLO IF: DATARESULT != EMPTY");

        if (priority == 0) {
            manager->getResultLpQueue()->push(dataresult);
        } else {
            manager->getResultHpQueue()->push(dataresult);
        }
        manager->change_token_results();
    }
}
////////////////////////////////////////////