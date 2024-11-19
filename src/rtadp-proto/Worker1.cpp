#include "Worker1.h"
#include "Supervisor.h"
#include "avro/Generic.hh"
#include "avro/Schema.hh"
#include "avro/ValidSchema.hh"
#include "avro/Compiler.hh"
#include "avro/GenericDatum.hh"
#include "avro/DataFile.hh"
#include "avro/Decoder.hh"
#include "avro/Specific.hh"

// Constructor
Worker1::Worker1() : WorkerBase() {
    // Load Avro schema from the provided schema string
    std::string avro_schema_str = R"({
        "type": "record",
        "name": "AvroMonitoringPoint",
        "namespace": "astri.mon.kafka",
        "fields": [
            {"name": "assembly", "type": "string"},
            {"name": "name", "type": "string"},
            {"name": "serial_number", "type": "string"},
            {"name": "timestamp", "type": "long"},
            {"name": "source_timestamp", "type": ["null", "long"]},
            {"name": "units", "type": "string"},
            {"name": "archive_suppress", "type": "boolean"},
            {"name": "env_id", "type": "string"},
            {"name": "eng_gui", "type": "boolean"},
            {"name": "op_gui", "type": "boolean"},
            {"name": "data", "type": {"type": "array", "items": ["double", "int", "long", "string", "boolean"]}}
        ]
    })";

    std::istringstream schema_stream(avro_schema_str);
    avro::compileJsonSchema(schema_stream, avro_schema);

    this->avro_schema = avro_schema;
}

// Override the config method
void Worker1::config(const nlohmann::json& configuration) {
    spdlog::error("Worker1::config: CONFIG");

    WorkerBase::config(configuration);
}

////////////////////////////////////////////
nlohmann::json Worker1::processData(const nlohmann::json& data, int priority) {
    // DEBUG
    spdlog::info("\n Received data: {}", data.dump());
    // spdlog::info("Worker1::processData: Supervisor pointer: {}", (get_supervisor() ? "Valid" : "Null"));

    nlohmann::json result;
    std::string dataflow_type = get_supervisor()->dataflowtype;
    std::cout << "Worker1::processData: dataflow_type: " << dataflow_type << std::endl;

    if (dataflow_type == "binary") {
        // Assuming data contains binary data as a string
        std::string binary_data = data.get<std::string>();
        std::unique_ptr<avro::InputStream> in = avro::memoryInputStream(
            reinterpret_cast<const uint8_t*>(binary_data.data()), binary_data.size()
        );

        // Create a binary decoder
        auto decoder = avro::binaryDecoder();
        decoder->init(*in);

        // Use GenericDatum to deserialize data
        avro::GenericDatum datum(avro_schema);
        avro::decode(*decoder, datum);

        if (datum.type() == avro::AVRO_RECORD) {
            const avro::GenericRecord& record = datum.value<avro::GenericRecord>();
            std::string name = record.field("name").value<avro::GenericDatum>().value<std::string>();
            result["name"] = name;
            std::cout << "Deserialized name: " << name << std::endl;
        }

        // Simulate processing
        std::this_thread::sleep_for(std::chrono::milliseconds(static_cast<int>(random_duration())));
    }
    else if (dataflow_type == "filename") {
        std::string filename = data.get<std::string>();
        // Simulate processing
        std::this_thread::sleep_for(std::chrono::milliseconds(static_cast<int>(random_duration())));
        result["filename"] = filename;
        std::cout << "Processed file: " << filename << std::endl;
    }
    else if (dataflow_type == "string") {
        std::cout << "PROCESSO STRINGHE" << std::endl;
        std::string str_data = data.get<std::string>();
        // std::string serialized_data = data.dump(); // Serializza l'intero JSON in una stringa

        result["data"] = str_data;
        std::cout << "\n Processed string data: " << str_data << std::endl;
    }

    result["priority"] = priority;
    return result;
}
////////////////////////////////////////////
// 
// Helper function to generate random duration between 0 and 100 milliseconds
double Worker1::random_duration() {
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_real_distribution<> dis(0.0, 100.0);
    return dis(gen);
}
