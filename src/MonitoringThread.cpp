// Copyright (C) 2024 INAF
// This software is distributed under the terms of the BSD-3-Clause license
//
// Authors:
//
//    Andrea Bulgarelli <andrea.bulgarelli@inaf.it>
//


#include "MonitoringThread.h"

using json = nlohmann::json;


// Constructor to initialize the MonitoringThread with a socket and MonitoringPoint reference
MonitoringThread::MonitoringThread(zmq::socket_t& socket_monitoring, MonitoringPoint& monitoringpoint)
    : socket_monitoring(socket_monitoring), monitoringpoint(monitoringpoint), stop_event(false) {
    std::cout << "Monitoring-Thread started" << std::endl;
}

// Destructor to stop the thread and clean up resources
MonitoringThread::~MonitoringThread() {
    stop();
    if (thread.joinable()) {
        thread.join();
    }
}

// Starts the monitoring thread
void MonitoringThread::start() {
    thread = std::thread(&MonitoringThread::run, this);
}

void MonitoringThread::stop() {
    stop_event = true;
}

void MonitoringThread::run() {
    while (!stop_event) {

        json monitoring_data = monitoringpoint.get_data();  // Get the current monitoring data

        std::string monitoring_data_str = monitoring_data.dump();  // Convert JSON to string

        zmq::message_t message(monitoring_data_str.begin(), monitoring_data_str.end());  // Create ZMQ message

        socket_monitoring.send(message, zmq::send_flags::none);  // Send the message through the socket
            

        std::this_thread::sleep_for(std::chrono::seconds(1));  // Sleep for 1 second
    }
}

// Sends monitoring data to a specific process target name
void MonitoringThread::sendto(const std::string& processtargetname) {
    json monitoring_data = monitoringpoint.get_data();  // Get the current monitoring data
    monitoring_data["header"]["pidtarget"] = processtargetname;  // Set the target process name
    std::string monitoring_data_str = monitoring_data.dump();  // Convert JSON to string
    zmq::message_t message(monitoring_data_str.begin(), monitoring_data_str.end());  // Create ZMQ message
    socket_monitoring.send(message, zmq::send_flags::none);  // Send the message through the socket
    std::cout << "send monitoring" << std::endl;
    std::cout << monitoring_data << std::endl;  // Print the monitoring data
}
