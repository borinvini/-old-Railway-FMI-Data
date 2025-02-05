import streamlit as st
import pandas as pd
import os
import seaborn as sns
import matplotlib.pyplot as plt
import numpy as np
import xgboost as xgb
from sklearn.decomposition import PCA
from sklearn.metrics import r2_score
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler

# Configuration
FOLDER_NAME = "output_data"

st.title("Weather Impact on Train Delays & Cancellations (PCA & XGBoost Analysis)")

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
# Standardize numeric columns
# ----------------------------------------------------------
numeric_cols = final_df.select_dtypes(include=[np.number]).columns
scaler = StandardScaler()
final_df[numeric_cols] = scaler.fit_transform(final_df[numeric_cols])

# ----------------------------------------------------------
# Apply PCA to reduce `differenceInMinutes` & `cancelled` into 1D
# ----------------------------------------------------------
st.subheader("Applying PCA to Reduce `differenceInMinutes` and `cancelled` to 1D")

# Convert `cancelled` to numeric
final_df["cancelled_numeric"] = final_df["cancelled"].astype(int)

# Select target columns
pca_targets = final_df[["differenceInMinutes", "cancelled_numeric"]].dropna()

# Apply PCA
pca = PCA(n_components=1)
pca_transformed = pca.fit_transform(pca_targets)

# Store the new 1D representation
final_df["PCA_Target"] = np.nan
final_df.loc[pca_targets.index, "PCA_Target"] = pca_transformed.flatten()

# Display PCA explained variance ratio
st.write(f"**Explained Variance by PCA:** {pca.explained_variance_ratio_[0]:.4f}")

# ----------------------------------------------------------
# Correlation Analysis with PCA Target
# ----------------------------------------------------------
weather_cols = [col for col in final_df.columns if col not in ["differenceInMinutes", "cancelled", "PCA_Target"]]

st.subheader("Correlation Analysis with PCA Target")

if not weather_cols:
    st.write("No weather variables found for correlation analysis.")
else:
    correlation_methods = ["pearson", "spearman", "kendall"]
    correlation_results_pca = {}

    for method in correlation_methods:
        corr_matrix_pca = final_df[["PCA_Target"] + weather_cols].corr(method=method)
        correlation_results_pca[method] = corr_matrix_pca.loc["PCA_Target", weather_cols].sort_values(ascending=False)

        # Display the correlation table
        st.write(f"### {method.capitalize()} Correlation with `PCA_Target`")
        corr_table = pd.DataFrame(correlation_results_pca[method]).reset_index()
        corr_table.columns = ["Weather Variable", f"{method.capitalize()} Correlation"]
        st.dataframe(corr_table)

    # ----------------------------------------------------------
    # Selecting the top 3 features from each correlation method
    # ----------------------------------------------------------
    top_features_pca = set()
    for method in correlation_methods:
        top_features_pca.update(correlation_results_pca[method].index[:3])

    top_features_pca = list(top_features_pca)

    # Display Selected Features in a Table
    st.write("### Selected Features for XGBoost (PCA Target)")
    st.dataframe(pd.DataFrame(top_features_pca, columns=["Feature"]), hide_index=True)

# ----------------------------------------------------------
# XGBoost Regression for `PCA_Target`
# ----------------------------------------------------------
st.subheader("XGBoost Regression for PCA-Transformed Target")

if not top_features_pca:
    st.write("Skipping regression analysis because no weather variables were found.")
else:
    reg_df = final_df[["PCA_Target"] + top_features_pca].dropna()

    if reg_df.empty:
        st.write("Not enough data after dropping missing values for regression analysis.")
    else:
        X = reg_df[top_features_pca]
        y = reg_df["PCA_Target"]

        # Split into 80% Train / 20% Test
        X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

        # Define XGBoost model
        model_pca = xgb.XGBRegressor(objective="reg:squarederror", n_estimators=10000, learning_rate=0.05, max_depth=5)
        model_pca.fit(X_train, y_train)

        # Generate Predictions
        y_train_pred = model_pca.predict(X_train)
        y_test_pred = model_pca.predict(X_test)

        # Calculate R² Scores
        r2_train = r2_score(y_train, y_train_pred)
        r2_test = r2_score(y_test, y_test_pred)

        # Display R² scores
        st.write(f"**Train R² Score:** {r2_train:.4f}")
        st.write(f"**Test R² Score:** {r2_test:.4f}")

        # Overfitting Warning
        if r2_train > r2_test + 0.1:
            st.write("⚠️ **Possible Overfitting!** Your model performs well on training data but poorly on test data.")
        else:
            st.write("✅ **Your model generalizes well!** No significant overfitting detected.")

        # ----------------------------------------------------------
        # Visualize Train vs. Test Predictions
        # ----------------------------------------------------------
        st.write("### Train vs. Test Predictions (XGBoost PCA)")
        fig, ax = plt.subplots(figsize=(8, 6))
        sns.scatterplot(x=y_train, y=y_train_pred, label="Train Data", alpha=0.5)
        sns.scatterplot(x=y_test, y=y_test_pred, label="Test Data", alpha=0.5, color="red")
        ax.set_xlabel("Actual PCA_Target")
        ax.set_ylabel("Predicted PCA_Target")
        ax.set_title("Train vs. Test Predictions (XGBoost PCA)")
        st.pyplot(fig)

        # ----------------------------------------------------------
        # Feature Importance
        # ----------------------------------------------------------
        feature_importance_pca = model_pca.feature_importances_
        importance_df_pca = pd.DataFrame({"Feature": top_features_pca, "Importance": feature_importance_pca})
        importance_df_pca = importance_df_pca.sort_values(by="Importance", ascending=False)

        st.write("### Most Influential Weather Features on PCA-Transformed Train Data")
        st.dataframe(importance_df_pca)

        # Feature Importance Plot
        st.write("### Feature Importance Plot")
        fig, ax = plt.subplots(figsize=(8, 6))
        sns.barplot(x="Importance", y="Feature", data=importance_df_pca, ax=ax)
        ax.set_title("Feature Importance (XGBoost PCA)")
        st.pyplot(fig)

# ----------------------------------------------------------
st.write("This analysis evaluates the impact of weather on train delays and cancellations using PCA transformation and XGBoost regression.")
