# Energy Production Forecasting in Denmark #

## About the project ##
The objective is to develop a compact interactive web app that provides energy production forecasts using machine learning or deep learning models. This app will use weather data to predict energy production at the municipality level in Denmark, with forecasts available in either hourly or daily intervals. The tool aims to display energy production predictions for the upcoming week.

This is intented to be a end-to-end machine learning project for improving our skills from data engineering all the way to production.
This means setting up APIs to get data, preprocessing and analyzing data, training perhaps both shallow - and deep learning models and building a small web app.
Decisions might be made just to try out and learn about technologies we haven't used, even if it might not be optimal.

**Possible technologies** 
- Data storage: SQL Server, maybe Azure?
- Data analysis and manipulation: SQL and Pandas(Polars instead?)
- Machine Learning: XGBoost
- Deep Learning: Pytorch, Huggingface
- Model & Data Tracking: MLflow
- Containerizer: Docker
- Webframework: Django, Streamlit, FastAPI, Flask


###  Prediction ###
Weather data for each municipality -> 
Daily Weather data(Temp, wind, wind direction,brigt sunshine) -> Energyproduction(solar, offshorewind,onshorewind, thermal)

### **Data** ###
Will start out by using data with a time-frequency of 1 day and split geographically by municipality
- ClimateData from DMI
    - Multiple time-frequencies.
    - Will use daily.
    - Original features(dateutc, municipality_id, municipality_name, mean_temp, mean_wind_speed, mean_wind_dir, bright_sunshine mean_relative_hum)
- Energy production data from Energi Data Service
    - Production per Municipality per Hour
        - Transform to daily
    - Time from January 2021 -> 27 Oct 2024
    - Original features(HourUTC, HourDK, MunicipalityNo, Solar, Offshorewind_lt_100mw, Offshorewind_ge_100mw, onshoreewind, thermalpower)
        - Unit is MWh



### **Preprocessing** ### 

### **Analysis** ###

### **Models** ###

### **Evaluation** ###

### **Webapp** ###


### Environment ###
To create the environment from .yml

```bash
conda env create -f environment.yml
``` 

To update the .yml

```bash
conda env export > environment.yml
```
