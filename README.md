

# Mini Project 10: Baseball Player Analysis with PySpark
![alt text](image.png)

## Project Purpose
This project analyzes historical baseball relief pitcher statistics using PySpark. It processes a large dataset (30,962 records) of baseball statistics, calculating various performance metrics and providing insights into pitcher and team performance across different eras.

## Project Data Source
The data used in this project comes FiveThirtyEight's public dataset: https://raw.githubusercontent.com/fivethirtyeight/data/refs/heads/master/goose/goose_rawdata.csv 

## Project Structure
```
Jennifer_Mini10_PySpark/
├── data/
│   └── goose.csv           
├── image/                   
├── mylib/
│   └── lib.py              
├── .devcontainer/
├── .github/                 
│   └── cicd.yml
├── .pytest_cache/          
├── .ruff_cache/           
├── Dockerfile               
├── Makefile                 
├── README.md                
├── main.py                  
├── requirements.txt        
├── test_main.py             
└── data_download.ipynb      
```

## Project Installation

1. Clone the repository
2. Install dependencies:
```bash
make install
```

## Project Usage
Run the analysis:
```bash
python main.py
```

The script will:
1. Load baseball statistics data
2. Calculate advanced metrics
3. Perform historical analysis
4. Save results in Parquet format

## Project Requirements and Implementation

✅ **Requirement 1: Use PySpark to perform data processing on a large dataset**
- Successfully processed 30,962 baseball statistics records
- Implemented distributed processing using PySpark DataFrame operations
- Handled multiple statistical calculations efficiently

![alt text](image-1.png)

![alt text](image-2.png)

✅ **Requirement 2: Spark SQL queries**
- Implemented three comprehensive SQL analyses, including one example below:

![alt text](image-4.png)

✅ **Requirement 3: Data transformations**
- Implemented multiple data transformations, including one example below:

![alt text](image-5.png)

## Project Outputs

![alt text](image-3.png)

![alt text](image-6.png)

## Testing Passed

![alt text](image-7.png)

## References
https://github.com/nogibjj/python-ruff-template 



