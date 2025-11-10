DATA VISUALISATION DASHBOARD
----------------------------

This project provides an interactive Python dashboard for analysing and visualising data. 
It is designed to help users quickly load, explore, and visualise large datasets (including millions of records) 
with a focus on flexibility, clarity, and ease of use.

FEATURES
- Flexible Data Loading:
  - Supports CSV, Excel, and TXT files with automatic delimiter detection.
  - Allow users to select the data directory and file interactively.

- Date Field Handling:
  - Auto-detects date columns based on user-specified or custom formats (including Unix epoch).
  - Offers interactive date filtering (by year, month, day of week, etc.).

- Column and Formula Selection:
  - Presents all available columns for selection.
  - Allows users to specify formulas for axes (e.g., `Quantity*Amount`).

- Visualisation Options:
  - Plots with grouping and aggregation (e.g., sum by product or date).
  - Automatic scaling and formatting of large numeric values (e.g., 1,000,000 → 1,000 ('000)).
  - Comma-separated axis values for readability.
  - Customisable axis and graph titles.

- User-Friendly Output:
  - All prompts and outputs are designed for clarity and ease of navigation.
  - Handles errors and invalid input gracefully.

HOW IT WORKS
1. Start the Dashboard:
   - Run the script. You will be prompted to select a data directory and file.
   - If file is in same directory as script, then no need to enter path

2. Date Field Detection:
   - The dashboard will help you identify date columns and select the correct format.

3. Visualisation:
   - Choose the type of visualisation.
   - Select X and Y axes (by column, number, or formula).
   - Optionally filter by date and group by categories.
   - Customise axis labels and graph title.
   - View the resulting plot with readable, well-formatted axes.

REQUIREMENT
- Python 3.7+
- pandas
- matplotlib
- seaborn
- squarify
- scipy

Install dependencies with:
pip install pandas matplotlib seaborn squarify scipy

Follow the on-screen prompts to load your data and generate visualisations.

PURPOSE
This dashboard is intended for business analysts, data scientists, and anyone who needs to quickly explore 
and visualise large datasets. It is especially useful for:
- Summarising by categories, date, or custom formula
- Identifying trends and patterns in large datasets
- Creating clear, publication-ready charts with minimal effort


License
MIT License

---
For questions, please contact aabanyie@gmail.com


