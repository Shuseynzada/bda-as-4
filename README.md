# Maritime Data Analysis with PySpark

This project leverages the capabilities of PySpark to analyze maritime data and identify the vessel that has traveled the longest route on a specific day. The analysis is performed using a Docker container to ensure a consistent and reproducible environment.

## Requirements

- Docker
- AIS Data file (e.g., `ais_data_2024-05-04.csv`)

## Getting Started

### 1. Clone the Repository

```bash
git clone https://github.com/Shuseynzada/bda-as-4.git
cd maritime-data-analysis
```

### 2. Place the Data File

Make sure the AIS data file (`ais_data_2024-05-04.csv`) is located in the project directory.

### 3. Build the Docker Image

```bash
docker build -t pyspark-tqdm .
```

### 4. Run the Docker Container

```bash
docker run -v ${PWD}:/app pyspark-tqdm
```

## Project Structure

```
.
├── Dockerfile
├── requirements.txt
├── main.py
└── ais_data_2024-05-04.csv
```

- **Dockerfile**: Defines the Docker image with PySpark and necessary dependencies.
- **requirements.txt**: Lists the Python dependencies.
- **main.py**: The main PySpark script to analyze the maritime data.
- **ais_data_2024-05-04.csv**: Example AIS data file (to be provided by the user).

## Script Description

The script performs the following steps:

1. Initializes a Spark session.
2. Loads the AIS data from a CSV file.
3. Filters out invalid latitude and longitude values.
4. Calculates the distance traveled by each vessel.
5. Identifies the vessel that traveled the longest distance.
6. Outputs the MMSI and the distance traveled by the vessel.

## Notes

- Ensure you have sufficient memory allocated to Docker to handle large datasets.
- Modify the `main.py` script if needed to accommodate different data structures or additional analysis requirements.

## License

This project is licensed under the MIT License.