# 📊 Value Props Analysis

## 🧩 Problem to Solve

Given:
- 📄 1-month history of prints shown to each user
- 👆 1-month history of taps made by each user
- 💳 1-month history of payments made by each user

Build a pipeline that:
- 📅 Displays prints from the last week
- 📌 For each print, includes:
  - ✅ A field indicating whether the user clicked or not
  - 🔁 Count of times the user saw each value prop in a 3-week window
  - 👆 Count of times the user clicked each value prop in a 3-week window
  - 💳 Number of payments made per value prop in a 3-week window
  - 💰 Accumulated amount spent per value prop in a 3-week window

## 🛠️ Solution

### 1. 🧵 Parse All Data Files into Pandas DataFrames
- Designed for performance and efficiency.
- Used **multithreading** since parsing is I/O-bound and tasks are independent.
- Avoided concurrent calls due to Python’s GIL limitations.

### 2. 🧮 Define Prints and Taps Computation
Since both share the same structure, the same logic applies:
- 📅 Extract date boundaries:
  - `most_recent_day`
  - `one_week_ago`
  - `three_weeks_ago`
- 🔄 Normalize the `event_data` JSON field to extract `value_prop`
- 🗑️ Drop the `position` column (not needed)
- 🧪 Generate subset:
  - Filter records within the 3-week window
  - Group by `user_id` and `value_prop` to count occurrences
- 🔗 Merge subset with original DataFrame, filling NaNs with 0

### 3. 💳 Define Payments Computation
Similar to prints/taps, but isolated for clarity:
- 📅 Extract date boundaries
- 🧪 Generate subset:
  - Filter records within the 3-week window
  - Group by `user_id` and `value_prop` to:
    - Count payments
    - Sum total amount
- 🔗 Merge subset with original DataFrame, filling NaNs with 0

### 4. ⚙️ Compute Prints, Taps, and Payments in Parallel
- Used **multiprocessing** since these are CPU-bound tasks
- Tasks are independent and benefit from parallel execution

### 5. 🧾 Generate the Final Dataset
- 🔗 Left-merge prints with taps on `user_id` and `value_prop`
- 🧼 Fill NaNs with 0 and cast counts to `int` (to avoid float inference)
- ✅ Create `clicked` column: `'yes'` if tap count > 0, else `'no'`
- 🔗 Left-merge with payments on `user_id` and `value_prop`
- 🧼 Fill NaNs with 0 and cast payment count to `int`
- 📅 Filter records from the last week
- 🗑️ Drop `day` column (no longer needed)
- 📊 Sort by `user_id`, `value_prop`, and `accumulated_amount` (to prioritize high-value props)
- 🔄 Reset index for a clean DataFrame
- 🧹 Remove duplicates:
  - Important to avoid redundant records
  - Example: multiple prints for `user_id = 1` and `value_prop = cellphone_recharge` on different days within the same week would reflect the same metrics—no need to keep both

### 6. 💾 Save Dataset
- Saving the dataset into the data folder
  <img width="764" height="640" alt="Screenshot 2025-08-19 at 12 42 16 PM" src="https://github.com/user-attachments/assets/21785de8-86e7-4abe-b66c-bac4c0a7a7e6" />


## 🚀 How to Run It

To get started with this project locally:

1. 📥 Clone the repository:
   ```bash
   git clone git@github.com:sechchr22/value_props_analisis.git
   cd value-props-analysis

2. 📦 Install dependencies:
    From the root directory.
    ```bash
    pip install -r analyzer/requirements.txt

3. ▶️ Run the pipeline:
    From the root directory.
    The output will be a clean dataset inside the data folder ready for analysis or integration.
    ```bash
    python3 -m analyzer.get_dataset
