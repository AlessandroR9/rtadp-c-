#ifndef SUPERVISOR_H
#define SUPERVISOR_H

#include <iostream>
#include <string>
#include <vector>
#include <zmq.hpp>
#include "json.hpp"
#include <thread>
#include <queue>
#include <csignal>
#include <unistd.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <fstream>
#include "WorkerLogger.h"
#include "ConfigurationManager.h"
#include "WorkerManager.h"

using json = nlohmann::json;


class Supervisor {

    // Helper function to decode data
    json decode_data(zmq::message_t &data);

    // Helper function to open file
    std::pair<std::vector<json>, int> open_file(const std::string &filename);

    // Static pointer to the current instance
    static Supervisor* instance;

    std::vector<std::string> worker_names;

public:
    // Constructor to initialize the Supervisor with configuration file and name
    Supervisor(std::string config_file = "config.json", std::string name = "None");

    // Destructor to clean up resources
    ~Supervisor();

    // Load configuration from the specified file and name
    void load_configuration(const std::string &config_file, const std::string &name);

    // Start service threads for data handling
    void start_service_threads();

    // Set up result channel for a given WorkerManager
    void setup_result_channel(WorkerManager *manager, int indexmanager);

    // Start managers
    virtual void start_managers();

    // Start workers
    void start_workers();

    // Start Supervisor operation
    virtual void start();

    // Static function to handle signals
    static void handle_signals(int signum);

    // Listen for result data
    void listen_for_result();

    // Send result data
    void send_result(WorkerManager *manager, int indexmanager);

    // Listen for low priority data
    void listen_for_lp_data();

    // Listen for high priority data
    void listen_for_hp_data();

    // Listen for low priority strings
    void listen_for_lp_string();

    // Listen for high priority strings
    void listen_for_hp_string();

    // Listen for low priority files
    void listen_for_lp_file();

    // Listen for high priority files
    void listen_for_hp_file();

    // Listen for commands
    void listen_for_commands();

    // Shutdown command
    void command_shutdown();

    // Cleaned shutdown command
    void command_cleanedshutdown();

    // Reset command
    void command_reset();

    // Start command
    void command_start();

    // Stop command
    void command_stop();

    // Start processing command
    void command_startprocessing();

    // Stop processing command
    void command_stopprocessing();

    // Start data command
    void command_startdata();

    // Stop data command
    void command_stopdata();

    // Process received command
    void process_command(const json &command);

    // Send alarm message
    void send_alarm(int level, const std::string &message, const std::string &pidsource, int code = 0, const std::string &priority = "Low");

    // Send log message
    void send_log(int level, const std::string &message, const std::string &pidsource, int code = 0, const std::string &priority = "Low");

    // Send info message
    void send_info(int level, const std::string &message, const std::string &pidsource, int code = 0, const std::string &priority = "Low");

    // Stop all threads and processes
    void stop_all(bool fast = false);

    // Static method to set the current instance
    static void set_instance(Supervisor *instance);

    // Static method to get the current instance
    static Supervisor* get_instance();

    WorkerLogger* getLogger() const {return logger; }

    std::string getName() const { return name; }

// Member variables
    std::string name;
    std::string fullname;
    std::string globalname;
    bool continueall;
    int pid;
    zmq::context_t context;
    zmq::socket_t *socket_lp_data;
    zmq::socket_t *socket_hp_data;
    zmq::socket_t *socket_command;
    zmq::socket_t *socket_monitoring;
    std::vector<zmq::socket_t*> socket_lp_result;
    std::vector<zmq::socket_t*> socket_hp_result;
    std::vector<std::string> getNameWorkers() const;
    WorkerLogger *logger;
    ConfigurationManager *config_manager;
    json config;
    int manager_num_workers;
    std::string manager_result_sockets_type;
    std::string manager_result_dataflow_type;
    std::vector<std::string> manager_result_lp_sockets;
    std::vector<std::string> manager_result_hp_sockets;
    std::string workername;
    std::vector<std::string> name_workers;
    std::string processingtype;
    std::string dataflowtype;
    std::string datasockettype;
    std::vector<WorkerManager*> manager_workers;
    int processdata;
    bool stopdata;
    std::string status;
    std::thread lp_data_thread;
    std::thread hp_data_thread;
    std::thread result_thread;
};

#endif // SUPERVISOR_H
