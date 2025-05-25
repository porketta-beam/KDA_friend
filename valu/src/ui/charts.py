import matplotlib.pyplot as plt
import streamlit as st

def setup_chart_style():
    plt.rcParams['font.family'] = 'Malgun Gothic'
    plt.rcParams['axes.unicode_minus'] = False

def plot_stock_price(df, selected_name):
    fig, ax = plt.subplots(figsize=(10, 3))
    ax.plot(df.index, df['Close'])
    ax.set_title(f"{selected_name} 종가 추이")
    ax.set_xlabel("날짜")
    ax.set_ylabel("종가 (원)")
    ax.grid(True)
    return fig 