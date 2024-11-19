// Copyright (C) 2024 INAF
// This software is distributed under the terms of the BSD-3-Clause license
//
// Authors:
//
//    Andrea Bulgarelli <andrea.bulgarelli@inaf.it>
//

#include "WorkerBase.h"

// Default constructor
WorkerBase::WorkerBase()
    : manager(nullptr), supervisor(nullptr) {  // Initialize pointers to nullptr
    // Logger is initialized in the init method
}

// Virtual destructor
WorkerBase::~WorkerBase() { 
    // Properly define the destructor, even if empty
}

// Initialize the worker with manager, supervisor, and names
void WorkerBase::init(WorkerManager* manager, Supervisor* supervisor, const std::string& workersname, const std::string& fullname) {
    this->manager = manager;
    this->supervisor = supervisor;
    // this->logger = spdlog::basic_logger_mt("worker_logger", "logs/worker.log");
    this->workersname = workersname;
    this->fullname = fullname;
    
    // spdlog::error("WorkerBase::init: INIZIALIZZATO");
}

void WorkerBase::config(const nlohmann::json& configuration) {
    // spdlog::error("WorkerBase::config: CONFIG");

    // Extract the pidtarget
    std::string pidtarget = configuration["header"]["pidtarget"];

    // Check if the configuration is meant for this worker
    if (pidtarget == workersname || pidtarget == fullname) {
        logger->info("Received config: {}", configuration.dump());
    }
}

nlohmann::json WorkerBase::processData(const nlohmann::json& data, int priority) {
   return {};
}
