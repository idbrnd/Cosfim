import requests
from datetime import datetime

def call_cosfim_api(table_name, dam_code, start_date, end_date):
    url = "http://cosfim.kwater.or.kr/COSFIMWebService/Provider.asmx"
    
    headers = {
        'Content-Type': 'text/xml; charset=utf-8',
        'SOAPAction': 'CDH.WebServices/ExecDataSet'
    }
    
    query = f'''select OBSDH "월일시분",SUM(RWL) "댐수위",SUM(EDQTY) "발전방류",SUM(ETCEDQTY) "기타발전",SUM(SPDQTY) "여수로방류",SUM(ETCDQTY1) "기타방류1",SUM(ETCDQTY2) "기타방류2",SUM(ETCDQTY3) "기타방류3",SUM(OTLTDQTY) "비상방류",SUM(ITQTY1) "취수1",SUM(ITQTY2) "취수2",SUM(ITQTY3) "취수3"  from DUBHRDAMIF where DAMCD = '{dam_code}' AND OBSDH between '{start_date}' and '{end_date}' group by OBSDH order by OBSDH asc'''
    print(query)
    xml_data = f'''<?xml version="1.0" encoding="utf-8"?>
<soap:Envelope xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xmlns:xsd="http://www.w3.org/2001/XMLSchema">
  <soap:Body>
    <ExecDataSet xmlns="CDH.WebServices">
      <tableName>{table_name}</tableName>
      <strQuery>{query}</strQuery>
      <type>OleDb</type>
    </ExecDataSet>
  </soap:Body>
</soap:Envelope>'''
    
    try:
        response = requests.post(url, headers=headers, data=xml_data)
        response.raise_for_status()  # HTTP 에러가 있으면 예외 발생
        return response.text
    except requests.exceptions.RequestException as e:
        print(f"요청 실패: {e}")
        return None

# 사용 예시
result = call_cosfim_api("합천댐", "2015110", "2025092307", "2025092407")
if result:
    print(result)