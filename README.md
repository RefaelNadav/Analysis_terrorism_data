
## Analysis terrorism data



### הוראות התקנה
1. שכפול המאגר:
```bash
git clone https://github.com/RefaelNadav/Analysis_terrorism_data.git
cd Analysis_terrorism_data
```

2. יצירת סביבת Python:
```bash
python -m venv venv
source venv/bin/activate  # Linux/Mac
# או
venv\Scripts\activate  # Windows
pip install -r requirements.txt
```



### מבנה המערכת
המערכת מורכבת משני שירותים עיקריים:

1. **Load data** 
   - הכנסת נתונים מהעבר על ארועי טרור

2. **Analysis Service**
   - סוגי התקפה קטלניים ביותר
   - מדינות עם ממוצע נפגעים גבוה ביותר (תצוגה במפה)
   - חמשת הקבוצות עם הכי הרבה נפגעים
   - אחוז שינוי בין שנים במספר אירועים לפי מדינה (תצוגה במפה)
   - הקבוצות הכי פעילות בכל מדינה
   - המטרה שהכי הרבה קבוצות תקפו לפי מדינה
   - סוג התקפה שהכי הרבה קבוצות השתמשו לפי מדינה


### API Endpoints

#### Home with map
```
GET /
```

#### Analysis Service
```
GET /api/analysis/deadliest
GET /api/analysis/top_groups
GET /api/analysis/diff_percentage
GET /api/analysis/active_groups
GET /api/analysis/common_target/<country>
GET /api/analysis/common_attack/<country>
```

