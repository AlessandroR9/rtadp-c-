#include "Supervisor1.h"

// Constructor
Supervisor1::Supervisor1(const std::string& config_file, const std::string& name)
    : Supervisor(config_file, name) {
}

// Destructor
Supervisor1::~Supervisor1() {
}


// Override the start_managers method
void Supervisor1::start_managers() {
    int indexmanager = 0;
    WorkerManager1* manager1 = new WorkerManager1(indexmanager, this, std::string(1, workername[indexmanager]));
    setup_result_channel(manager1, indexmanager);
    manager1->run();
    manager_workers.push_back(manager1);
    std::cout << "DER SUP manager started. man lenght: " << manager_workers.size() << std::endl;
}

// Decode the data before loading it into the queue. For "dataflowtype": "binary"
zmq::message_t& Supervisor1::decode_data(zmq::message_t& data) {
    return data;
}

// Open the file before loading it into the queue. For "dataflowtype": "file"
// Return an array of data and the size of the array
std::pair<std::vector<std::string>, int> Supervisor1::open_file(const std::string& filename) {
    std::vector<std::string> f = {filename};
    return {f, 1};
}
