# 🌾 Crop Portfolio Optimizer
### AI-Driven Decision Intelligence for Smart Farming

<img width="1888" height="797" alt="image" src="https://github.com/user-attachments/assets/3793084b-73f8-4dd3-82a0-ab4b40851346" />


The **Crop Portfolio Optimizer** is an AI-powered decision support system that helps farmers determine **what crops to grow and how much land to allocate** based on environmental conditions, historical agricultural data, and intelligent reasoning.

Instead of relying on intuition alone, the system analyzes multiple data sources such as **historical yield statistics and weather conditions** to recommend a **risk-balanced crop portfolio** that improves profitability and resilience.

This project demonstrates how **AI agents, real-world agricultural APIs, and full-stack web technologies** can work together to support smarter agricultural planning.

---

# 🚜 Problem Statement

Farmers often make planting decisions based on:

- past experience  
- limited weather insights  
- uncertain agricultural data  

However, agriculture is highly sensitive to:

- climate variability  
- soil conditions  
- historical yield patterns  

Poor crop planning can lead to:

- inefficient land utilization  
- reduced productivity  
- financial losses  

The goal of this project is to build a **data-driven crop planning assistant** that recommends an optimal crop mix based on **real agricultural and climate data**.

---

# 🎯 Project Goal

Given a farmer’s:

- farm location  
- total land size  
- crops under consideration  

The system recommends:

- optimal crop allocation  
- land distribution per crop  
- expected yield potential  
- climate suitability  
- a risk-balanced crop portfolio

  

---

# 🧠 System Architecture

The system combines **AI agents, external APIs, and full-stack web architecture**.

---

Farmer Input (React Frontend)->  Backend API Layer -> Decision Agent (API Selection) -> External Data Retrieval -> Data Processing & Crop Suitability Analysis
-> Portfolio Optimization -> Validator Agent -> Final Crop Recommendation
         
---

# 🤖 Multi-Agent Architecture

The system uses a **multi-agent design** to intelligently retrieve data and validate decisions.

Each agent performs a specialized task in the workflow.

---

## 1️⃣ Decision Agent — API Selection

The **Decision Agent** determines which external APIs should be used for a given farmer query.

Instead of calling all data sources, the agent dynamically selects **only the relevant APIs**.

### Responsibilities

- interpret farmer input
- determine required datasets
- select appropriate APIs
- trigger the data retrieval pipeline

### Example

**User Input**
Location: Ohio
Farm Size: 100 acres
Crops: Corn, Soybean, Wheat


**Decision Agent selects**

| Data Requirement | API |
|---|---|
| Historical Crop Yield | USDA NASS API |
| Weather & Climate | NOAA Weather API |

This improves efficiency and ensures **context-aware data retrieval**.

---

## 2️⃣ Data Collection Layer

After API selection, the system retrieves agricultural and environmental data.

Collected data includes:

- historical crop yields  
- production statistics  
- weather patterns  
- climate indicators  

This data forms the foundation for crop analysis.

---

## 3️⃣ Crop Suitability Analysis

The system analyzes the collected data to determine how suitable each crop is for the given environment.

Evaluation factors include:

- historical yield trends  
- geographic suitability  
- climate compatibility  
- crop stability over time  

The system generates **crop suitability scores** that guide the optimization engine.

---

## 4️⃣ Crop Portfolio Optimization Engine

Instead of recommending a single crop, the optimizer creates a **balanced crop portfolio** to reduce farming risk.

Factors considered:

- yield stability  
- diversification benefits  
- environmental compatibility  
- land constraints  

Example output:

| Crop | Land Allocation | Confidence |
|---|---|---|
Corn | 40% | High |
Soybean | 35% | Medium |
Wheat | 25% | Medium |

<img width="1853" height="803" alt="image" src="https://github.com/user-attachments/assets/e7c9b78c-bca7-47b3-8ddb-601f32bbdd43" />

---

## 5️⃣ Validator Agent — Result Verification

The **Validator Agent** checks whether the generated recommendation is logically valid.

This acts as a **quality assurance layer** for AI decisions.

### Responsibilities

- verify crop allocation totals
- validate yield predictions
- ensure results match historical data
- detect anomalies or unrealistic outputs

### Example validation rules

| Check | Requirement |
|---|---|
Total land allocation | Must equal farm size |
Crop suitability | Must align with weather data |
Yield prediction | Must fall within historical ranges |

If inconsistencies are detected:
→ system re-runs optimization
→ allocation is adjusted
→ corrected recommendation is generated


---

# 📊 Data Sources

The system integrates real agricultural datasets.

---

## USDA NASS Quick Stats API

Provides historical agricultural statistics.

Collected fields include:

- crop name  
- year  
- state / county  
- yield per acre  
- harvested area  
- production statistics  

Purpose:  
Used for **historical yield analysis**.

---

## NOAA Weather Data

Provides environmental and climate insights.

Collected data includes:

- temperature trends  
- rainfall patterns  
- seasonal climate variations  

Purpose:  
Used to evaluate **weather suitability for crop growth**.

---

# ⚙️ Technology Stack

## Frontend

- React.js  
- JavaScript  
- CSS / UI components  

Purpose:  
Provides an interactive interface where farmers can input farm details and view crop recommendations.

---

## Backend

- Python  
- Node.js / API layer  

Purpose:  
Handles API calls, agent execution, optimization logic, and communication with the database.

---

## Database

- MongoDB  

Purpose:

- stores farm inputs
- saves generated crop portfolios
- maintains historical analysis results

---

## Data Processing

- Pandas  
- NumPy  

---

## External APIs

- USDA NASS QuickStats API  
- NOAA Weather API  

---

# 🧩 Example Workflow

1️⃣ Farmer enters:
Location: Ohio
Farm Size: 100 acres
Crops: Corn, Soybean, Wheat


2️⃣ Decision Agent selects required APIs.

3️⃣ System retrieves:

- crop yield data
- weather conditions

4️⃣ Crop suitability analysis is performed.

5️⃣ Portfolio optimization generates crop allocation.

6️⃣ Validator Agent checks the results.

7️⃣ Final recommendation is stored in MongoDB and displayed in the UI.

---

# 📈 Example Output
Recommended Crop Portfolio

Corn: 40 acres
Soybean: 35 acres
Wheat: 25 acres

Expected Yield Stability: High
Climate Risk Exposure: Moderate

<img width="1804" height="794" alt="image" src="https://github.com/user-attachments/assets/4a0ef612-d2b3-4fa7-a9d0-996f31599a3a" />


---

# 💡 Real-World Impact

This project demonstrates how **AI and data analytics can support modern agriculture**.

Potential benefits:

- improved farm profitability  
- better land utilization  
- reduced climate risk  
- data-driven farming strategies  

---

# 🔬 Future Improvements

Possible extensions include:

- commodity price prediction  
- soil health integration  
- satellite crop monitoring  
- machine learning yield forecasting  
- crop rotation planning  
- risk simulation models  

---



