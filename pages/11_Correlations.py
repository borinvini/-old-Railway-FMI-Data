import streamlit as st
import pandas as pd
import os
import seaborn as sns
import matplotlib.pyplot as plt
import numpy as np
import xgboost as xgb
from sklearn.metrics import r2_score, accuracy_score
from sklearn.model_selection import train_test_split

# Configuration
FOLDER_NAME = "output_data"

st.title("Weather Impact on Train Delays & Cancellations (Correlation & XGBoost Analysis)")

# ----------------------------------------------------------
# Load all 12 final_corr_data_XX.csv files into one DataFrame.
# ----------------------------------------------------------
st.write("Loading Files...")
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
            st.write(f"Error loading file {file_name}: {e}")

if not df_list:
    st.error("No data files were loaded. Please check the files in the output_data folder.")
    st.stop()

st.write("Files Loaded ✅")

# Combine all DataFrames
final_df = pd.concat(df_list, ignore_index=True)

# Replace any None values with np.nan so that Pandas ignores them
final_df = final_df.replace({None: np.nan})

# Ensure `cancelled` is a boolean type
final_df["cancelled"] = final_df["cancelled"].astype(bool)

# ----------------------------------------------------------
# Standardize every numeric column using z-score normalization.
# ----------------------------------------------------------
numeric_cols = final_df.select_dtypes(include=[np.number]).columns
final_df[numeric_cols] = final_df[numeric_cols].apply(lambda x: (x - x.mean()) / x.std())

# ----------------------------------------------------------
# Advanced Correlation Analysis for `differenceInMinutes` & `cancelled`
# ----------------------------------------------------------
weather_cols = [col for col in final_df.columns if col not in ["differenceInMinutes", "cancelled"]]

st.subheader("Advanced Correlation Analysis")

if not weather_cols:
    st.write("No weather variables found for correlation analysis.")
else:
    correlation_methods = ["pearson", "spearman", "kendall"]
    correlation_results_diff = {}
    correlation_results_cancelled = {}

    for method in correlation_methods:
        # Correlation for `differenceInMinutes`
        corr_matrix_diff = final_df[["differenceInMinutes"] + weather_cols].corr(method=method)
        correlation_results_diff[method] = corr_matrix_diff.loc["differenceInMinutes", weather_cols].sort_values(ascending=False)

        st.write(f"### {method.capitalize()} Correlation with `differenceInMinutes`")
        corr_table = pd.DataFrame(correlation_results_diff[method]).reset_index()
        corr_table.columns = ["Weather Variable", f"{method.capitalize()} Correlation"]
        st.dataframe(corr_table)

        # Correlation for `cancelled` (Converted to numeric)
        final_df["cancelled_numeric"] = final_df["cancelled"].astype(int)
        corr_matrix_cancelled = final_df[["cancelled_numeric"] + weather_cols].corr(method=method)
        correlation_results_cancelled[method] = corr_matrix_cancelled.loc["cancelled_numeric", weather_cols].sort_values(ascending=False)

        st.write(f"### {method.capitalize()} Correlation with `cancelled`")
        corr_table = pd.DataFrame(correlation_results_cancelled[method]).reset_index()
        corr_table.columns = ["Weather Variable", f"{method.capitalize()} Correlation"]
        st.dataframe(corr_table)

    # ----------------------------------------------------------
    # Selecting the top 3 features from each correlation method
    # ----------------------------------------------------------
    top_features_diff = set()
    top_features_cancelled = set()

    for method in correlation_methods:
        top_features_diff.update(correlation_results_diff[method].index[:3])
        top_features_cancelled.update(correlation_results_cancelled[method].index[:3])

    top_features_diff = list(top_features_diff)
    top_features_cancelled = list(top_features_cancelled)

    # Display Selected Features in a Table
    st.write("### Selected Features for XGBoost (Train Delays)")
    st.dataframe(pd.DataFrame(top_features_diff, columns=["Feature"]), hide_index=True)

    st.write("### Selected Features for XGBoost (Train Cancellations)")
    st.dataframe(pd.DataFrame(top_features_cancelled, columns=["Feature"]), hide_index=True)

# ----------------------------------------------------------
# XGBoost Regression for `differenceInMinutes`
# ----------------------------------------------------------
st.subheader("XGBoost Regression for Train Delays (`differenceInMinutes`)")

if not top_features_diff:
    st.write("Skipping regression analysis because no weather variables were found.")
else:
    reg_df = final_df[["differenceInMinutes"] + top_features_diff].dropna()

    if reg_df.empty:
        st.write("Not enough data after dropping missing values for regression analysis.")
    else:
        X = reg_df[top_features_diff]
        y = reg_df["differenceInMinutes"]

        X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

        model_diff = xgb.XGBRegressor(objective="reg:squarederror", n_estimators=10000, learning_rate=0.01, max_depth=5)
        model_diff.fit(X_train, y_train)

        y_train_pred = model_diff.predict(X_train)
        y_test_pred = model_diff.predict(X_test)

        r2_train = r2_score(y_train, y_train_pred)
        r2_test = r2_score(y_test, y_test_pred)

        st.write(f"**Train R² Score:** {r2_train:.4f}")
        st.write(f"**Test R² Score:** {r2_test:.4f}")

        # Plot: Actual vs. Predicted Values
        fig, ax = plt.subplots(figsize=(8, 6))
        sns.scatterplot(x=y_train, y=y_train_pred, label="Train Data", alpha=0.5)
        sns.scatterplot(x=y_test, y=y_test_pred, label="Test Data", alpha=0.5, color="red")
        ax.set_xlabel("Actual differenceInMinutes")
        ax.set_ylabel("Predicted differenceInMinutes")
        ax.set_title("Train vs. Test Predictions (XGBoost)")
        st.pyplot(fig)

# ----------------------------------------------------------
# XGBoost Classification for `cancelled`
# ----------------------------------------------------------
st.subheader("XGBoost Classification for Train Cancellations (`cancelled`)")

if not top_features_cancelled:
    st.write("Skipping classification analysis because no weather variables were found.")
else:
    class_df = final_df[["cancelled"] + top_features_cancelled].dropna()

    if class_df.empty:
        st.write("Not enough data after dropping missing values for classification analysis.")
    else:
        X = class_df[top_features_cancelled]
        y = class_df["cancelled"].astype(int)

        X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

        model_cancelled = xgb.XGBClassifier(n_estimators=10000, learning_rate=0.01, max_depth=5)
        model_cancelled.fit(X_train, y_train)

        y_train_pred = model_cancelled.predict(X_train)
        y_test_pred = model_cancelled.predict(X_test)

        acc_train = accuracy_score(y_train, y_train_pred)
        acc_test = accuracy_score(y_test, y_test_pred)

        st.write(f"**Train Accuracy:** {acc_train:.4f}")
        st.write(f"**Test Accuracy:** {acc_test:.4f}")

        # Feature Importance Plot
        feature_importance_cancelled = model_cancelled.feature_importances_
        importance_df_cancelled = pd.DataFrame({"Feature": top_features_cancelled, "Importance": feature_importance_cancelled})
        importance_df_cancelled = importance_df_cancelled.sort_values(by="Importance", ascending=False)

        st.write("### Most Influential Weather Features on Train Cancellations")
        st.dataframe(importance_df_cancelled)

        fig, ax = plt.subplots(figsize=(8, 6))
        sns.barplot(x="Importance", y="Feature", data=importance_df_cancelled, ax=ax)
        ax.set_title("Feature Importance (XGBoost - Train Cancellations)")
        st.pyplot(fig)

# ----------------------------------------------------------
st.write("This analysis evaluates the impact of weather on train delays and cancellations using correlation and XGBoost models.")
