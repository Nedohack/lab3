from tasks import process_data

if __name__ == "__main__":

    input_register = "../input/register.csv"
    input_stations = "../input/stations.csv"
    output_path = "../output/critical_stations"


    threshold = 0.25

    process_data(input_register, input_stations, output_path, threshold)