import streamlit as st
from b_sidebar import render_sidebar_content
from b_main import render_main_content

def main():
    st.set_page_config(
    page_title="va?lu",
    page_icon=":dolphin:",
    layout="centered",
    initial_sidebar_state="auto",
    )
    render_sidebar_content()
    
    render_main_content()
    

if __name__ == "__main__":
    main()
