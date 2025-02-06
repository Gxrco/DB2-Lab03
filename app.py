from neo4j import GraphDatabase

uri = "<URI>"         
username = "<USERNAME>"                      
password = "<PASSWORD>"              

driver = GraphDatabase.driver(uri, auth=(username, password))

def close_connection():
    driver.close()

def verify_connection():
    try:
        with driver.session() as session:
            result = session.run("RETURN 1 AS value")
            value = result.single()["value"]
            print("Connection successful. Result:", value)
    except Exception as e:
        print("Connection failed:", e)

verify_connection()