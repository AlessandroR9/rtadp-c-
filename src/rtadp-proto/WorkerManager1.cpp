#include "WorkerManager1.h"

// Constructor
WorkerManager1::WorkerManager1(int manager_id, Supervisor* supervisor, const std::string& name)
    : WorkerManager(manager_id, supervisor, name), manager_id(manager_id) {}

// Override to start worker threads
void WorkerManager1::start_worker_threads(int num_threads) {
    // WorkerManager::start_worker_threads(num_threads);
    // Create worker threads
    for (int i = 0; i < num_threads; ++i) {
        auto processor = std::make_shared<Worker1>();
        auto thread = std::make_shared<WorkerThread>(i, this, getSupervisor()->getNameWorkers()[manager_id], processor.get());
        worker_threads.push_back(thread);
        thread->run();  // Start the thread
    }
}

// Override to start worker processes
void WorkerManager1::start_worker_processes(int num_processes) {
    WorkerManager::start_worker_processes(num_processes);
    // Create worker processes
    for (int i = 0; i < num_processes; ++i) {
        auto processor = std::make_shared<Worker1>();
        auto process = std::make_shared<WorkerProcess>(i, shared_from_this(), getSupervisor()->getNameWorkers()[manager_id], processor);
        getWorker_Processes().push_back(process);
        process->run();  // Start the process
    }
}
