import streamlit as st
import pandas as pd
from trino import dbapi
import plotly.express as px
# Trino
conn = dbapi.connect(
    host='localhost', 
    port=8060,
    user='trino',
    catalog='iceberg',
    schema='silver'
)
cur = conn.cursor()

# Streamlit app
st.title("Ecom Sales Dashboard")

tab1, tab2= st.tabs(["Sales","Sellers"])

with tab1:
    st.header("Sales")

    # Sales by product category
    query = """
    SELECT p.product_category_name_english, SUM(f.price) as total_sales
    FROM Fact_Sales f
    JOIN Dim_Products p ON f.product_id = p.product_id
    GROUP BY p.product_category_name_english
    ORDER BY total_sales DESC
    LIMIT 20
    """

    try:
        cur.execute(query)
        df = pd.DataFrame(cur.fetchall(), columns=['product_category', 'total_sales']) 
    except Exception as e:
        st.error(f"Failed to execute query: {e}")
        st.stop()

    df = df.sort_values('total_sales', ascending=False)

    fig = px.bar(df, x='total_sales', y='product_category', orientation='h', labels={'total_sales':'Total Sales', 'product_category':'Product Category'})
    st.plotly_chart(fig)

    query = """
    SELECT 
        DATE_TRUNC('month', o.order_purchase_timestamp) as month, 
        SUM(f.price) as total_sales
    FROM 
        Fact_Sales f
    JOIN 
        Dim_Orders o ON f.order_id = o.order_id
    GROUP BY 
        DATE_TRUNC('month', o.order_purchase_timestamp)  -- Group by the calculated month
    ORDER BY 
        DATE_TRUNC('month', o.order_purchase_timestamp)  -- Order by the calculated month
    """

    try:
        cur.execute(query)
        df = pd.DataFrame(cur.fetchall(), columns=['month', 'total_sales']) 
    except Exception as e:
        st.error(f"Failed to execute query: {e}")
        st.stop()

    fig = px.line(df, x='month', y='total_sales', labels={'month':'Month', 'total_sales':'Total Sales'})
    st.plotly_chart(fig)


    query = """
    SELECT p.payment_type, SUM(f.price) as total_sales
    FROM Fact_Sales f
    JOIN iceberg.bronze.order_payments p ON f.order_id = p.order_id
    GROUP BY p.payment_type
    """

    try:
        cur.execute(query)
        df = pd.DataFrame(cur.fetchall(), columns=['payment_type', 'total_sales']) 
    except Exception as e:
        st.error(f"Failed to execute query: {e}")
        st.stop()

    fig = px.pie(df, values='total_sales', names='payment_type', title='Sales by Payment Type')
    st.plotly_chart(fig)



# Close the connection
cur.close()
conn.close()