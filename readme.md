#CPCB Data For Bangalore
- Using [Advanced search feature on CPCB website](http://www.cpcb.gov.in/CAAQM/frmUserAvgReportCriteria.aspx)
- Collected frequency level 4 hrs
- Stations BTM, BWSSB, Peenya, SGHALLI, CRS
- All raw xls converted to into CSVs are inside data/raw folder
- Cleaned and organized db is in data/db/data.sqlite3
- Final csvs are in data/final
- data.csv - has data, by datetime and station level, one column per type of observation
- metadata.csv - has all the details of raw files, stations, frequency etc
- parameters.csv - has full_name, short_name, and unit. Short names are also used as column names for parameters in data table and data.csv

## Data Version
1.0 - Collected for years 2015, 2016 and 2017 (Till Aug)
1.1 - Added Sept, Oct, Nov 2017 Data

## Credit
- Credit CPCP for Data, Thejesh GN for cleaning and organizing. 
- You can credit CPCB by linking to http://www.cpcb.gov.in/CAAQM/frmUserAvgReportCriteria.aspx
- You can credit Thejesh GN by linking to https://thejeshgn.com
- Data organization is under ODBL license 
 
