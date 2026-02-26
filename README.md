# Behavioral Analytics Engine

![Java](https://img.shields.io/badge/Java-21-ED8B00?style=for-the-badge&logo=openjdk&logoColor=white)
![Apache Spark](https://img.shields.io/badge/Apache%20Spark-4.1.1-E25A1C?style=for-the-badge&logo=Apache%20Spark&logoColor=white)
![Maven](https://img.shields.io/badge/Maven-C71A36?style=for-the-badge&logo=Apache%20Maven&logoColor=white)
![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg?style=for-the-badge)

A high-performance data processing pipeline built with Apache Spark and Java that analyzes customer behavioral metrics. This engine processes raw interaction logs and utilizes a multi-sink output strategy to serve both automated downstream systems and human analysts.

## 🚀 Features

* **Data Aggregation:** Calculates the average success rate of customer interactions, grouped by geographic region and service tier.
* **Multi-Sink Output Strategy:**
  * **System-Optimized:** Writes highly compressed, optimized Parquet files for ingestion by downstream machine learning models or data warehouses.
  * **Analyst-Ready:** Generates formatted, human-readable plain text reports for immediate business review.
* **Optimized Execution:** Leverages Spark's untyped `Dataset<Row>` API and native column-based functions to maximize performance via the Catalyst Optimizer, avoiding unnecessary object serialization overhead.

## 🛠 Tech Stack

* **Language:** Java 21
* **Framework:** Apache Spark 4.1.1
* **Build Tool:** Maven
* **Data Formats:** CSV (Input), Parquet (Output), TXT (Output)

## 📂 Project Structure

```text
behavioral-analytics-engine/
├── src/main/java/com/criscoded/analytics/
│   ├── Main.java               # Driver class and Spark Session initialization
│   └── engine/
│       └── AnalyticsEngine.java # Core data pipeline and multi-sink logic
├── src/main/resources/
│   └── customer_behavior_log.csv # Raw input data
├── output/behavioral_report/     # Auto-generated output directory
└── pom.xml                       # Maven dependencies and build configuration

A high-performance data processing pipeline built with Apache Spark and Java that analyzes customer behavioral metrics. This engine processes raw interaction logs and utilizes a multi-sink output strategy to serve both automated downstream systems and human analysts.

## 🚀 Features

* **Data Aggregation:** Calculates the average success rate of customer interactions, grouped by geographic region and service tier.
* **Multi-Sink Output Strategy:**
  * **System-Optimized:** Writes highly compressed, optimized Parquet files for ingestion by downstream machine learning models or data warehouses.
  * **Analyst-Ready:** Generates formatted, human-readable plain text reports for immediate business review.
* **Optimized Execution:** Leverages Spark's untyped `Dataset<Row>` API and native column-based functions to maximize performance via the Catalyst Optimizer, avoiding unnecessary object serialization overhead.

## 🛠 Tech Stack

* **Language:** Java 21
* **Framework:** Apache Spark 4.1.1
* **Build Tool:** Maven
* **Data Formats:** CSV (Input), Parquet (Output), TXT (Output)

## 📂 Project Structure

```text
behavioral-analytics-engine/
├── src/main/java/com/criscoded/analytics/
│   ├── Main.java               # Driver class and Spark Session initialization
│   └── engine/
│       └── AnalyticsEngine.java # Core data pipeline and multi-sink logic
├── src/main/resources/
│   └── customer_behavior_log.csv # Raw input data
├── output/behavioral_report/     # Auto-generated output directory
└── pom.xml                       # Maven dependencies and build configuration
```

## ⚙️ Getting Started

**Prerequisites**
* Java Development Kit (JDK) 21
* Apache Maven

**Installation & Execution**
1. Clone the repository:

    > Bash
    ```
    git clone [https://github.com/criscoded/behavioral-analytics-engine.git](https://github.com/criscoded/behavioral-analytics-engine.git)
    cd behavioral-analytics-engine
    ```

2. Build the executable JAR:  
     Use Maven to compile the Java code, download the Spark dependencies, and package everything into a single executable "fat JAR".

    > Bash
    ```
    mvn clean package
    ```

4. Run the application:

    You can run the Main.java class directly through your IDE, or execute the compiled jar via the command line. The Spark local master local[*] is configured by default for testing.
   > Bash
   ```
   java -jar target/customer-behavioral-analytics-1.0-SNAPSHOT.jar
   ```
   (Alternatively, you can run the Main.java class directly through your preferred IDE).

 **Output**

 Upon successful execution, the pipeline will generate an output/behavioral_report/ directory at the project root containing:
 * The distributed ```.snappy.parquet``` files
 * A ```behavioral-output.txt``` file containing the formatted analyst report

## ![Results](https://img.shields.io/badge/Results-8A2BE2?style=for-the-badge&logo=googleanalytics&logoColor=white)

![SuccessConfirmation](https://github.com/user-attachments/assets/4493891f-62f1-48e1-bff0-1340ec54e7fb)
![TextOutput](https://github.com/user-attachments/assets/4a26b036-8596-42fe-8add-ebbd3de4bb69) 


## 📝 License  
Distributed under the MIT License. See LICENSE for more information.
