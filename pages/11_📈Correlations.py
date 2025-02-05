import streamlit as st
import pandas as pd
import os
import seaborn as sns
import matplotlib.pyplot as plt
import numpy as np
import xgboost as xgb
from sklearn.metrics import r2_score
from sklearn.model_selection import train_test_split

# Configuration
FOLDER_NAME = "output_data"

st.title("Weather Impact on Train Delays")

# ----------------------------------------------------------
# Load all 12 final_corr_data_XX.csv files into one DataFrame.
# ----------------------------------------------------------
st.write("Loading data...")
df_list = []
months = [str(i).zfill(2) for i in range(1, 13)]

for month in months:
    file_name = f"final_corr_data_{month}.csv"
    file_path = os.path.join(os.getcwd(), FOLDER_NAME, file_name)
    if os.path.exists(file_path):
        try:
            temp_df = pd.read_csv(file_path)
            df_list.append(temp_df)
        except Exception as e:
            st.write(f"Error loading {file_name}: {e}")

if not df_list:
    st.error("No data files were loaded. Check the 'output_data' folder.")
    st.stop()

st.write("Data loaded successfully ✅")

# Combine all DataFrames
final_df = pd.concat(df_list, ignore_index=True)

# Drop the 'cancelled' column
if "cancelled" in final_df.columns:
    final_df = final_df.drop(columns=["cancelled"])

# Replace any None values with NaN
final_df = final_df.replace({None: np.nan})

# ----------------------------------------------------------
# Show data before normalization
# ----------------------------------------------------------
st.write("Raw data preview")
st.dataframe(final_df)

# ----------------------------------------------------------
# Standardize every numeric column using z-score normalization.
# ----------------------------------------------------------
numeric_cols = final_df.select_dtypes(include=[np.number]).columns
final_df[numeric_cols] = final_df[numeric_cols].apply(lambda x: (x - x.mean()) / x.std())

# ----------------------------------------------------------
# Show data after normalization
# ----------------------------------------------------------
st.write("Normalized data preview")
st.dataframe(final_df)

# ----------------------------------------------------------
# Correlation Analysis for differenceInMinutes
# ----------------------------------------------------------
weather_cols = [col for col in final_df.columns if col != "differenceInMinutes"]

st.write("Correlation analysis")

if not weather_cols:
    st.write("No weather variables found for correlation analysis.")
else:
    correlation_methods = ["pearson", "spearman", "kendall"]
    correlation_results = {}

    for method in correlation_methods:
        # Compute correlation with differenceInMinutes
        corr_matrix = final_df[["differenceInMinutes"] + weather_cols].corr(method=method)
        correlation_results[method] = corr_matrix.loc["differenceInMinutes", weather_cols].sort_values(ascending=False)

        st.write(f"{method.capitalize()} correlation with differenceInMinutes")
        corr_table = pd.DataFrame(correlation_results[method]).reset_index()
        corr_table.columns = ["Weather Variable", f"{method.capitalize()} Correlation"]
        st.dataframe(corr_table)

    # ----------------------------------------------------------
    # Select the top 3 features from each correlation method
    # ----------------------------------------------------------
    top_features = set()
    for method in correlation_methods:
        top_features.update(correlation_results[method].index[:3])

    top_features = list(top_features)

    # Display Selected Features in a Table
    st.write("Selected features for XGBoost")
    st.dataframe(pd.DataFrame(top_features, columns=["Feature"]), hide_index=True)

# ----------------------------------------------------------
# XGBoost Regression for differenceInMinutes
# ----------------------------------------------------------
st.write("XGBoost regression for train delays")

if not top_features:
    st.write("Skipping regression analysis due to lack of features.")
else:
    reg_df = final_df[["differenceInMinutes"] + top_features].dropna()

    if reg_df.empty:
        st.write("Not enough data after dropping missing values.")
    else:
        X = reg_df[top_features]
        y = reg_df["differenceInMinutes"]

        X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

        model = xgb.XGBRegressor(objective="reg:squarederror", n_estimators=10000, learning_rate=0.01, max_depth=10)
        model.fit(X_train, y_train)

        y_train_pred = model.predict(X_train)
        y_test_pred = model.predict(X_test)

        r2_train = r2_score(y_train, y_train_pred)
        r2_test = r2_score(y_test, y_test_pred)

        st.write(f"Train R² Score: {r2_train:.4f}")
        st.write(f"Test R² Score: {r2_test:.4f}")

        # ----------------------------------------------------------
        # Plot: Actual vs. Predicted Values
        # ----------------------------------------------------------
        fig, ax = plt.subplots(figsize=(8, 6))
        sns.scatterplot(x=y_train, y=y_train_pred, label="Train Data", alpha=0.5)
        sns.scatterplot(x=y_test, y=y_test_pred, label="Test Data", alpha=0.5, color="red")
        ax.set_xlabel("Actual differenceInMinutes")
        ax.set_ylabel("Predicted differenceInMinutes")
        ax.set_title("Train vs. Test Predictions (XGBoost)")
        st.pyplot(fig)

        # ----------------------------------------------------------
        # Feature Importance Plot
        # ----------------------------------------------------------
        feature_importance = model.feature_importances_
        importance_df = pd.DataFrame({"Feature": top_features, "Importance": feature_importance})
        importance_df = importance_df.sort_values(by="Importance", ascending=False)

        st.write("Most influential weather features on train delays")
        st.dataframe(importance_df)

        fig, ax = plt.subplots(figsize=(8, 6))
        sns.barplot(x="Importance", y="Feature", data=importance_df, ax=ax)
        ax.set_title("Feature Importance (XGBoost - Train Delays)")
        st.pyplot(fig)

st.write("Analysis completed: Weather impact on train delays evaluated using correlation and XGBoost.")