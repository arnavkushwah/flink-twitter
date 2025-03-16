import streamlit as st
# from twitter_producer import get_results
import plotly.express as px
from flink_processor import word_count


######################################## Sample  data ########################################

sentiment_counts = {
    "Positive": 200,
    "Neutral": 30,
    "Negative": 50,
}
######################################## Sample  data ########################################

######################################## Running Flink ########################################
keyword_results, location_results, favorite_results = word_count()
######################################## Running Flink ########################################


st.set_page_config(page_title="Live Twitter Summary Dashboard", layout="wide")
st.title("📊 Twitter Summary Dashboard")
st.write("This dashboard was built on top of Apache Flink, Twitter API, as well as Open AI's API, to display live summaries of Twitter trends based on selected topics.")

# User input to search up a specific topic (uncomment when fixed)
user_input = st.text_input("Search up any topic here: ")
# results = get_results(user_input)
# st.write(results)

# Overall sentiment pie chart 
st.header("Overall Sentiment")
data = [{"Sentiment": k, "Count": v} for k, v in sentiment_counts.items()]
fig = px.pie(
    data,
    names="Sentiment",
    values="Count",
    color="Sentiment",
    color_discrete_map={"Positive": "green", "Neutral": "gray", "Negative": "red"},
    title=" ",
    hole=0.5,
)

# Customize layout
fig.update_layout(
    title_font_size=20,
    font=dict(size=14),
    paper_bgcolor="rgba(0,0,0,0)",  # Transparent background
    showlegend=True,
)
st.plotly_chart(fig)

# Showing the top tweets
st.header("🐥 Top tweets")
cols = st.columns(2)
for i, tweet in enumerate(favorite_results):
    with cols[i % 2]:  # Distribute tweets in two columns
        st.markdown(f"""
        **User:** {tweet[2]}  
        📝 **Tweet:** {tweet[0]}  
        ❤️ {tweet[1]} Likes | 🔁 {tweet[3]} Retweets  
        """)
        st.divider()


# Showing the top keywords
st.header("🔝 Top keywords")
cols = st.columns(2)
for i, keyword in enumerate(keyword_results):
    with cols[i % 2]:  # Distribute keywords in two columns
        st.markdown(f"""
        🔑 **Keyword:** {keyword[0]}  
        📊 **Mentions:** {keyword[1]}  
        """)
        st.divider()

# Showing the top locations
st.header("🗺️ Where people are talking from")
cols = st.columns(2)
for i, location in enumerate(location_results):
    with cols[i % 2]:  # Distribute keywords in two columns
        st.markdown(f"""
        📍 **Location:** {location[0]}  
        📊 **Tweet Count:** {location[1]}  
        """)
        st.divider()

