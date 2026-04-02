# 🌾 Crop Portfolio Optimizer
### An AI-Powered Profit Optimization Framework for Agricultural Decision Support

<img width="1919" height="817" alt="image" src="https://github.com/user-attachments/assets/8f02d36a-ed2d-4924-ba6f-f85f57930e66" />


Most high-stakes decisions share the same problem: profitability depends on multiple moving factors, incomplete information, and scattered data — yet decisions still get made on gut instinct and fragmented research.

We built a **Profit Optimization Model** to change that. It identifies the key value drivers behind a decision, stress-tests multiple scenarios, and surfaces which path creates the strongest financial outcome.

**Farming is our first implementation — and the fit is deliberate.**

Agricultural decisions sit at the intersection of yield variability, weather, soil conditions, commodity prices, and input costs — all at once. Few domains match that analytical complexity. Fewer still offer the kind of public data infrastructure that USDA, NOAA, and NRCS provide: decades of structured, accessible, real-world data that lets us build a working product, not just a concept.

The framework underneath is built to go further — into finance, real estate, insurance, healthcare, and beyond.

---

## Executive Summary

Crop Portfolio Optimizer helps farmers evaluate crop choices through a profitability lens.

The platform brings together:

- Historical agricultural data
- Weather and climate context
- Soil compatibility signals
- Market pricing inputs
- Cost assumptions
- Scenario-based profit analysis

The result is a system that goes beyond recommending a crop. It helps users understand expected profit, yield outlook, risk, and the reasoning behind each recommendation.

---

## Why This Project Matters

Many important decisions are still made through fragmented research, manual comparison, and one-point assumptions.

That creates three common problems:

- Too much time spent gathering information
- Limited visibility into trade-offs
- Weak decision confidence when conditions change

This project addresses that gap by turning scattered inputs into a structured decision-support workflow.

In agriculture, that means helping farmers move from fragmented planning to more informed and financially grounded crop decisions.

At a broader level, it demonstrates how a **profit optimization framework** can be translated into a usable product for business decision-making.

---

## The Architecture: Built to Generalize

<img width="1892" height="958" alt="image" src="https://github.com/user-attachments/assets/d56421b3-93dd-4266-9c9f-cbca08ea8784" />


The system is not a farming tool that happens to use AI. It is a **decision-support architecture** that happens to be deployed in farming first.

At its core, the architecture follows a repeatable pattern:

```
User Context Input
        │
        ▼
 Decision Agent
 (Orchestration Layer)
 • Interprets the decision problem
 • Identifies required data signals
 • Structures inputs for analysis
        │
        ▼
 Intelligent Agent
 (Analysis & Recommendation Layer)
 • Applies the Profit Optimization Model
 • Evaluates options across scenarios
 • Ranks outcomes by profitability and risk
        │
        ▼
 Structured Recommendation Output
 • Expected outcome (P50)
 • Range of outcomes (P10 / P90)
 • Risk score
 • Ranked alternatives
 • Supporting reasoning
```

This two-agent pipeline is **domain-agnostic by design**. The Decision Agent can be re-instructed to interpret a different kind of decision problem. The Intelligent Agent can apply the same profit optimization logic to a different set of variables. The output layer can surface the same structured outputs — profit range, scenario comparison, risk score — regardless of industry.

What changes between sectors is the **data layer and the domain-specific variables**. The agent architecture, the optimization model, and the recommendation logic remain consistent.

---

## Business Value

The value of this system is not just a recommendation. It is **better decision quality**.

Instead of asking users to collect and compare everything on their own, the platform helps structure the decision by showing:

- What the likely financial outcome looks like
- Which assumptions are shaping the recommendation
- Where the upside and downside sit
- How options compare against each other

That creates value by helping users:

- Reduce time spent gathering information from multiple sources
- Compare alternatives on a common financial basis
- Understand trade-offs before acting
- Make more profit-aware decisions
- See ranges instead of relying on one-point estimates
- Move from reactive planning to more structured planning

In farming specifically, this supports:

- Better crop selection
- Clearer profitability comparison
- Stronger land-use planning
- Better visibility into weather, soil, and market conditions
- More confidence when making planning decisions

The same business logic can extend to other industries where profitability depends on multiple uncertain drivers and scenario-based trade-offs.

---

## Core Concept: Profit Optimization Model

At the center of the system is a simple financial structure:

$$\text{Profit} = \text{Expected Revenue} - \text{Expected Cost}$$

Where revenue can be expressed as:

$$\text{Expected Revenue} = \text{Forecasted Output} \times \text{Expected Price}$$

This basic structure is then extended through scenario-based analysis.

Because real decisions are not static, the model accounts for changes in:

- Output outlook
- Market movement
- External conditions
- Suitability factors
- Cost assumptions

Instead of producing one fixed answer, the model evaluates outcomes under multiple conditions.

### Scenario Structure

The model stress-tests outcomes through:

| Scenario | Description |
|---|---|
| **Best-case** | Favorable yield, price, and conditions |
| **Base-case** | Expected outcome under typical conditions |
| **Worst-case** | Adverse yield, price, or external conditions |

### Output Structure

The system produces:

- **Profit range** — the spread between downside and upside
- **Expected outcome** — the base-case profit projection
- **Risk level** — how sensitive the result is to changing conditions

This makes the model more useful for decision-making because it not only identifies what appears profitable. It also shows how sensitive that result may be when the surrounding conditions shift.

---

## Cross-Industry Relevance

The framework behind this project is not agriculture-specific.

The same decision structure can be used in industries where profitability depends on multiple changing drivers.

| Industry | Application |
|---|---|
| **Finance** | Compare return, downside risk, and market movement across investment choices |
| **Real Estate** | Evaluate revenue potential, cost, occupancy assumptions, and market conditions |
| **Insurance** | Compare pricing, exposure, and risk across different scenarios |
| **Healthcare** | Compare resource allocation, cost, and expected operational outcomes |
| **SaaS / Product Strategy** | Compare investment, pricing, adoption, and expected return across product decisions |

Across all of these areas, the pattern remains consistent:

1. Identify the key drivers
2. Define assumptions
3. Test different scenarios
4. Compare which option creates the strongest financial outcome

---

## Product Experience

### Landing Experience

The landing experience positions the platform around four core capabilities:

- **Profit Optimization**
- **Weather Intelligence**
- **Market Analysis**
- **Risk Assessment**

It also highlights the integrated data foundation behind the system, including agricultural, climate, soil, and market-related inputs.

<img width="1919" height="817" alt="image" src="https://github.com/user-attachments/assets/4501ed18-1a90-416e-9e8e-5778b58ca631" />


### Dashboard Experience

Once inside the application, users can:

- Launch a new farm analysis
- Review recent analyses
- Track their latest recommendation
- Revisit prior decision history

This makes the platform useful for ongoing planning, not just one-time use.

<img width="1916" height="843" alt="image" src="https://github.com/user-attachments/assets/cad9357e-c9b8-4fb4-ac65-3994bae7f270" />


### Recommendation Experience

The recommendation view is designed to support interpretation, not just output delivery.


It presents:

- **Top recommendation**
- **Expected profit**
- **Yield forecast**
- **Risk score**
- **Crop rankings**
- **Soil compatibility**
- **Profit distribution**
- **Weather summary**
- **Market outlook**

This allows the user to understand not only which crop is recommended, but also the reasoning behind that recommendation.

<img width="1629" height="847" alt="image" src="https://github.com/user-attachments/assets/bbbed907-0acf-47b6-b27f-a49b9d83a502" />

---

## How the System Works

The platform follows a **two-agent workflow**.

### 1. User Input

The user provides the farm profile and decision context.

This can include:

- Acreage
- Location
- Soil type
- Candidate crops
- Other relevant farm inputs

### 2. Decision Agent

The first agent acts as the **decision and orchestration layer**.

Its responsibilities include:

- Interpreting the user input
- Determining what supporting data is required
- Identifying the relevant external signals
- Preparing structured inputs for downstream analysis

This stage builds the context required for a meaningful recommendation.

### 3. Intelligent Agent

The second agent acts as the **analysis and recommendation layer**.

Its responsibilities include:

- Applying the profit optimization model
- Evaluating crop-level outcomes
- Comparing options under different scenarios
- Assessing profitability and risk
- Generating ranked recommendations

### 4. Recommendation Delivery

The final output is presented through the dashboard in an interpretable format that supports decision-making.

---

## Why Agentic AI Makes Sense Here

This project uses agentic AI in a practical way.

The two-agent design is not there for novelty. It serves a clear purpose.

- The **Decision Agent** decides what information is needed based on the user's input.
- The **Intelligent Agent** takes that structured input and turns it into analysis, comparison, and recommendation.

This makes the workflow more modular, more interpretable, and more useful than a one-step recommendation pipeline.

---

## Data Sources

The system integrates public agricultural and environmental datasets.

| Source | Purpose |
|---|---|
| **USDA NASS** | Historical yield and production data |
| **NOAA** | Weather and climate context |
| **NRCS** | Soil-related signals |
| **ERS / public cost inputs** | Cost assumption baselines |
| **Market pricing signals** | Commodity-related price inputs |

These inputs support:

- Yield estimation
- Weather interpretation
- Soil compatibility analysis
- Cost assumptions
- Price outlook generation

---

## Recommendation Outputs

The system currently supports outputs such as:

- **Top crop recommendation**
- **Expected profit**
- **P10 / P50 / P90 profit distribution**
- **Yield forecast**
- **Price forecast**
- **Revenue per acre**
- **Profit per acre**
- **Soil compatibility**
- **Risk score**
- **Weather summary**
- **Market outlook**

### P10 / P50 / P90 Interpretation

For each crop, the model displays a range of possible profit outcomes:

| Percentile | Meaning |
|---|---|
| **P10** | Lower-end profit outcome (downside scenario) |
| **P50** | Median / expected profit outcome (base case) |
| **P90** | Higher-end profit outcome (upside scenario) |

This allows the user to compare not only expected profitability, but also the spread between downside and upside outcomes.
<img width="761" height="372" alt="image" src="https://github.com/user-attachments/assets/db7d5d2b-5d0d-4b2c-9683-c331611d9a03" />

<img width="718" height="381" alt="image" src="https://github.com/user-attachments/assets/e6df249e-bc73-43d3-a961-dc902877116f" />

<img width="722" height="689" alt="image" src="https://github.com/user-attachments/assets/c4c01636-7c25-4196-8946-07ba08d8f7e5" />


---

## Model Limitations

This is **not a perfect model**, and the project is explicit about that.

Perfect accuracy is difficult because the system depends on public data sources that are valuable but not always ideal for precise forecasting.

### Key limitations include

- Unstructured government and public datasets
- Lagged cost estimates
- Volatile commodity prices
- Incomplete data coverage
- Uneven real-time availability

For that reason, the system was not designed to create a false impression of exact precision.

Instead, it was built around:

- **Directional accuracy**
- **Scenario ranges**
- **Transparent flags**
- **Interpretable outputs**

The goal is to support better decisions under imperfect conditions, not to overstate forecasting certainty.

---

## Technology Stack

### Frontend

- React.js
- JavaScript
- Modern UI components / CSS

### Backend

- Python
- Node.js / API orchestration layer

### Data and Analytics

- Pandas
- NumPy

### External APIs and Data Services

- USDA NASS QuickStats
- NOAA weather and climate inputs
- NRCS soil data
- Market and cost-related public inputs

---

## Running the Project Locally

This repository includes **example environment files** to show the structure of the required configuration.

To run the project locally, you must create your **own actual `.env` files** and provide your own credentials, keys, and configuration values.

### Important

- Example env files are provided only as templates
- They do **not** contain working secrets or usable credentials
- You must create your own local `.env` files before running the project
- API keys, database connection strings, and private configuration must be added by the user

### Typical setup flow

1. Copy the example env files
2. Rename them to the expected `.env` file names
3. Add your own credentials and configuration values
4. Save the files locally
5. Run the application

### Example

```bash
cp .env.example .env
```
