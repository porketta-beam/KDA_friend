import streamlit as st

def initialize_session_state():
    if 'initialized' not in st.session_state:
        st.session_state.initialized = True
        st.session_state.chat_history = []
        st.session_state.active_sidebar = None
        st.session_state._chat_submitted = False

def handle_chat_submit():
    st.session_state._chat_submitted = True 