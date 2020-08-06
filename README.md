# Wildfire Automation -- Finds Wildfires and Alerts the Control Center

Complimentary to my Oil Spill Finder project that I worked on in Summer 2019 as a Software Engineer for Chevron

This automation runs hourly at Chevron and does
1. Webscrapes a NASA website to find the most recent satellite data link (updates every hour)
2. Runs a get request in order to get the data in the form of CSV
3. Processes the data by using geospatial analysis to determine severity and uniqueness
4. Adds those severe instances to a Database
5. Sends an email alert to the control center with all severe instances and their relevant data
