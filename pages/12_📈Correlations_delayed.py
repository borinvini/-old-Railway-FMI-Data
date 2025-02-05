import streamlit as st
import pandas as pd
import os
import seaborn as sns
import matplotlib.pyplot as plt
import numpy as np
import xgboost as xgb
from sklearn.metrics import accuracy_score
from sklearn.model_selection import train_test_split
from imblearn.over_sampling import SMOTE  
from sklearn.metrics import confusion_matrix

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
# Create 'delayed' column and remove 'differenceInMinutes'
# ----------------------------------------------------------
final_df["delayed"] = final_df["differenceInMinutes"] > 0  # Boolean column
final_df = final_df.drop(columns=["differenceInMinutes"])  # Remove original column

# Move 'delayed' column to the first position
cols = ["delayed"] + [col for col in final_df.columns if col != "delayed"]
final_df = final_df[cols]

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
# Correlation Analysis for `delayed`
# ----------------------------------------------------------
weather_cols = [col for col in final_df.columns if col != "delayed"]

st.write("Correlation analysis")

if not weather_cols:
    st.write("No weather variables found for correlation analysis.")
else:
    correlation_methods = ["pearson", "spearman", "kendall"]
    correlation_results = {}

    for method in correlation_methods:
        # Compute correlation with `delayed`
        final_df["delayed_numeric"] = final_df["delayed"].astype(int)  # Convert to numeric for correlation
        corr_matrix = final_df[["delayed_numeric"] + weather_cols].corr(method=method)
        correlation_results[method] = corr_matrix.loc["delayed_numeric", weather_cols].sort_values(ascending=False)

        st.write(f"{method.capitalize()} correlation with `delayed`")
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
# XGBoost Classification for `delayed` with SMOTE
# ----------------------------------------------------------
st.write("XGBoost classification for train delays (delayed) with SMOTE")

if not top_features:
    st.write("Skipping classification analysis due to lack of features.")
else:
    reg_df = final_df[["delayed"] + top_features].dropna()

    if reg_df.empty:
        st.write("Not enough data after dropping missing values.")
    else:
        X = reg_df[top_features]
        y = reg_df["delayed"].astype(int)  # Convert boolean to integer for classification

        # Split data into train and test sets
        X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42, stratify=y)

        # Apply SMOTE to balance the dataset
        smote = SMOTE(sampling_strategy="auto", random_state=42)
        X_train_smote, y_train_smote = smote.fit_resample(X_train, y_train)

        # Train XGBoost Model
        model = xgb.XGBClassifier(n_estimators=10000, learning_rate=0.01, max_depth=10)
        model.fit(X_train_smote, y_train_smote)

        # Predictions
        y_train_pred = model.predict(X_train_smote)
        y_test_pred = model.predict(X_test)

        # Accuracy Scores
        acc_train = accuracy_score(y_train_smote, y_train_pred)
        acc_test = accuracy_score(y_test, y_test_pred)

        st.write(f"Train Accuracy: {acc_train:.4f}")
        st.write(f"Test Accuracy: {acc_test:.4f}")


        # ----------------------------------------------------------
        # Confusion Matrix
        # ----------------------------------------------------------
        cm = confusion_matrix(y_test, y_test_pred)

        fig, ax = plt.subplots(figsize=(6, 4))
        sns.heatmap(cm, annot=True, fmt="d", cmap="Blues", xticklabels=["Not Delayed", "Delayed"], yticklabels=["Not Delayed", "Delayed"])

        ax.set_xlabel("Predicted Label")
        ax.set_ylabel("Actual Label")
        ax.set_title("Confusion Matrix - XGBoost (Train Delays)")

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
        ax.set_title("Feature Importance (XGBoost - Train Delays with SMOTE)")
        st.pyplot(fig)


st.write("Analysis completed: Weather impact on train delays evaluated using correlation and XGBoost with SMOTE.")
