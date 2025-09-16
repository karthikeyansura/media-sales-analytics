# Media Sales Analytics Datamart

Developed as part of CS5200 – Practicum II at Northeastern University, this project demonstrates the full pipeline for designing and analyzing a cloud-hosted data warehouse, integrating film and music sales for advanced reporting and analytics.

---

## Key Features
- **Cloud-Hosted Analytics Database:** Configured a MySQL database on the cloud to support star schema–based reporting.
- **Star Schema Design:** Created fact and dimension tables integrating data from both the **film-sales** and **music-sales** SQLite databases.
- **ETL Pipeline:** Developed R scripts to extract data from two operational databases, transform it into a unified structure, and load it into the analytical data warehouse.
- **Pre-computed Fact Tables:** Implemented aggregate measures for revenue, units sold, and customer counts by country, product type, and time period (month/quarter/year).
- **Efficient Analytics:** Applied indexing and partitioning strategies to improve query performance on large-scale datasets.
- **Business Reporting:** Generated a HTML report with R Markdown, providing sales insights for Media Distributors, Inc.

---

## Database Schema (Star Schema)

The analytics schema follows a **star schema design**, integrating dimensions for:
- **Time** (month, quarter, year)
- **Country**
- **Customer Type** (music vs. film)
- **Sales Facts** (revenue, units sold, aggregated measures)

---

## Repository Structure
```
media-sales-analytics/
│── createStarSchema.PractII.SuraS.R      # Creates fact and dimension tables (star schema)
│── loadAnalyticsDB.PractII.SuraS.R       # Runs ETL pipeline to populate analytics warehouse
│── BusinessAnalysis.PractII.SuraS.Rmd    # R Markdown report with visual analytics
│── deleteSchema.PractII.SuraS.R          # Drop all tables in schema
│── ConfCloudAnalyticsDB.PractII.SuraS.R  # Cloud MySQL connection configuration
│── film-sales.db                         # Operational database (film sales)
│── music-sales.db                        # Operational database (music sales)
│── README.md
```

---

## How to Run

### Prerequisites
-   R and RStudio installed.
-   Access to a cloud-hosted MySQL database (e.g., AWS RDS, Aiven, db4free).
-   Update the database connection credentials in all `.R` and `.Rmd` files.

### Execution Order
Run the scripts in the following sequence from within RStudio:

1. Clone repo:
   ```bash
   git clone https://github.com/karthikeyansura/media-sales-analytics
   ```
2. **`createStarSchema.PractII.SuraS.R`**: Creates the fact and dimension tables (star schema).
3. **`loadAnalyticsDB.PractII.SuraS.R`**: Runs the ETL pipeline to populate the analytics datamart.
4. **Knit `BusinessAnalysis.PractII.SuraS.Rmd`**: Generates the final HTML report with analytics.
5. (Optional) Run **`deleteSchema.PractII.SuraS.R`** to clear the schema.

---

## Business Insights

The analytics report provides:
- Revenue trends by country and product type (film vs. music)
- Units sold (total and average) across time periods
- Customer counts by country and type
- Aggregated statistics (min, max, average, total) for both units and revenue