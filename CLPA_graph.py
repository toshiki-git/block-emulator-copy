import numpy as np
import matplotlib.pyplot as plt
import networkx as nx

# アカウント数とシャード数を定義
N = 4
K = 2

# エッジの重みを定義
# e[i][j] はアカウント i と j の間のトランザクション数を示します
e = np.array([
    [0, 3, 2, 0],  # A
    [3, 0, 0, 5],  # B
    [2, 0, 0, 4],  # C
    [0, 5, 4, 0]   # D
])

# アカウントのシャード割り当てを定義
# x[i][k] はアカウント i がシャード k に割り当てられているかどうかを示します
# 不均衡なパーティション
x_unbalanced = np.array([
    [1, 0],  # A
    [1, 0],  # B
    [0, 1],  # C
    [0, 1]   # D
])


# クロスシャードトランザクションのワークロードを計算する関数
def calculate_cross_shard_tx_workload(e, x):
    N, K = x.shape
    C = 0
    for i in range(N):
        for j in range(N):
            cross_shard = 1 - sum(x[i, k] * x[j, k] for k in range(K))
            C += e[i, j] * cross_shard
    return C / 2

# 各シャードのワークロードを計算する関数
def calculate_shard_workloads(e, x):
    N, K = x.shape
    L = np.zeros(K)
    
    for k in range(K):
        for l in range(K):
            if l != k:
                for i in range(N):
                    for j in range(N):
                        L[k] += e[i, j] * x[i, k] * x[j, l]
        
        for i in range(N):
            for j in range(N):
                L[k] += (e[i, j] * x[i, k] * x[j, k]) / 2
    
    return L

# ワークロード不均衡指数を計算する関数
def calculate_workload_imbalance(L):
    avg_workload = np.mean(L)
    D = np.max(np.abs(L - avg_workload))
    return D

# 計算結果を表示
def display_results(e, x, title):
    C = calculate_cross_shard_tx_workload(e, x)
    L = calculate_shard_workloads(e, x)
    D = calculate_workload_imbalance(L)
    print(title)
    print("クロスシャードトランザクションのワークロード(C):", C)
    print("各シャードのワークロード(L):", L)
    print("ワークロード不均衡指数(D):", D)

# グラフを描画する関数
def draw_graph(e, title):
    G = nx.Graph()
    labels = ['A', 'B', 'C', 'D']
    
    # ノードの追加
    for i, label in enumerate(labels):
        G.add_node(i, label=label)
    
    # エッジの追加
    for i in range(len(e)):
        for j in range(len(e)):
            if e[i, j] > 0:
                G.add_edge(i, j, weight=e[i, j])
    
    pos = nx.spring_layout(G)  # ノードの配置を決定
    edge_labels = {(i, j): f"{e[i, j]}" for i, j in G.edges()}  # エッジの重みをラベルとして設定

    plt.figure()
    nx.draw(G, pos, with_labels=True, labels={i: labels[i] for i in G.nodes()})
    nx.draw_networkx_edge_labels(G, pos, edge_labels=edge_labels)
    plt.title(title)
    plt.show()

display_results(e, x_unbalanced, "不均衡なパーティション")
draw_graph(e, "不均衡なパーティションのグラフ")

