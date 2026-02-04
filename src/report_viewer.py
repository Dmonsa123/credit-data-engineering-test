import pandas as pd
import matplotlib.pyplot as plt
import os

def generar_grafico():
    path_resumen = "data/gold/resumen_riesgo.csv"
    output_image = "report/kpi_riesgo.png"
    
    os.makedirs("report", exist_ok=True)

    if not os.path.exists(path_resumen):
        print(f"⚠️ No se encontró {path_resumen}. Saltando gráfico.")
        return

    df = pd.read_csv(path_resumen)

    plt.figure(figsize=(10, 6))
    colors = ['#ff9999','#66b3ff','#99ff99'] # Rojo, Azul, Verde para riesgo
    plt.bar(df['risk_segment'], df['outstanding_balance'], color=colors)
    
    plt.title('Exposición Financiera por Segmento de Riesgo', fontsize=14)
    plt.xlabel('Segmento de Riesgo')
    plt.ylabel('Balance Pendiente')
    plt.grid(axis='y', alpha=0.3)

    # GUARDAR Y CERRAR
    plt.savefig(output_image)
    print(f"✅ Gráfico guardado en '{output_image}'")
    
    # plt.show() # <-- Comentado para automatización
    plt.close() # <-- Importante para liberar memoria en el main