# librerias
import mysql.connector
import datetime
import pandas as pd
import numpy as np
from sklearn.preprocessing import StandardScaler
from sklearn.cluster import KMeans
import seaborn as sns
from kmodes.kmodes import KModes
import pickle
import matplotlib.pyplot as plt

# datos de Octubre (mes pasado)
today = datetime.date.today()
last_month = today - datetime.timedelta(days=30)
last_month_year = last_month.strftime("%Y")
last_month_month = last_month.strftime("%m")

# creo conexion
cnx = mysql.connector.connect(
    user='pbi_read_user',
    password='qEdrupH9wrOjEbra',
    host='gharg-wp-prd.cfozgrysvlfs.us-east-1.rds.amazonaws.com',
    database='gharg-wp-prd',
    port=3306
)

# creo cursor
cursor = cnx.cursor()

# consulta todos los ids de marzo de 2023
query = f"""
SELECT ID FROM pr_2_posts 
WHERE post_type = 'shop_order' and post_status = 'wc-bill' and YEAR(post_date) = {last_month_year} and MONTH(post_date) = {last_month_month}
ORDER BY post_date DESC;
"""
cursor.execute(query)

# creo lista con ventas
post_id_ventas_hasta_mes_actual = [i[0] for i in cursor]

# creo Dataframe
ventas_hasta_mes_actual = pd.DataFrame()
ventas_hasta_mes_actual['post_id'] = post_id_ventas_hasta_mes_actual

# mail
def buscar_cuil(df):
    """
    Recibe un df con post_id de ventas y devuelve otro df con el cuil correspondiente a cada venta
    """
    query = f"""
    SELECT post_id, meta_value FROM pr_2_postmeta 
    WHERE post_id in {tuple(df.post_id.values)} and meta_key = '_billing_email';
    """
    cursor.execute(query)
    resultado = cursor.fetchall()
    cuil = pd.DataFrame(resultado, columns=['post_id', 'mail'])
    return df.merge(cuil)

ventas_hasta_mes_actual = buscar_cuil(ventas_hasta_mes_actual)

# comprobantes
query = f"""
SELECT order_id, id, type, creation_date FROM pr_2_pmi_bills 
WHERE order_id in {tuple(ventas_hasta_mes_actual.post_id.values)};
"""
cursor.execute(query)
resultado = cursor.fetchall()
bills = pd.DataFrame(resultado, columns=['post_id', 'bill_id', 'type', 'fecha'])

# añado el mail
ventas_hasta_mes_actual = bills.merge(ventas_hasta_mes_actual, how='left')

# prod por bill id
query = f"""
SELECT bill_id, product_id, quantity FROM pr_2_pmi_bill_items 
WHERE bill_id in {tuple(ventas_hasta_mes_actual.bill_id.values)};
"""
cursor.execute(query)
productos_vendidos_hasta_mes_actual = cursor.fetchall()
productos_vendidos_hasta_mes_actual = pd.DataFrame(productos_vendidos_hasta_mes_actual, columns=['bill_id', 'product_id', 'quantity'])

# descripcion
query = f"""
SELECT id, post_title, post_content FROM pr_2_posts 
WHERE id in {tuple(productos_vendidos_hasta_mes_actual.product_id.values)};
"""
cursor.execute(query)
prod_descripcion = cursor.fetchall()
prod_descripcion = pd.DataFrame(prod_descripcion, columns=['product_id', 'titulo', 'descripcion'])

# cigarrillos u otros prod
query = f"""
SELECT post_id, meta_value FROM pr_2_postmeta 
WHERE post_id in {tuple(productos_vendidos_hasta_mes_actual.product_id.values)} and meta_key = 'tipo_prd_id';
"""
cursor.execute(query)
tipo_prd_id = cursor.fetchall()
tipo_prd_id = pd.DataFrame(tipo_prd_id, columns=['product_id', 'tipo_prd_id'])
prod_descripcion = prod_descripcion.merge(tipo_prd_id)
productos_vendidos_hasta_mes_actual = productos_vendidos_hasta_mes_actual.merge(prod_descripcion, how='left')

# replico datos de facturante
df = productos_vendidos_hasta_mes_actual.merge(ventas_hasta_mes_actual, how='left')
df['bill_id'] = pd.to_numeric(df['bill_id'])
df['product_id'] = pd.to_numeric(df['product_id'])
df['quantity'] = pd.to_numeric(df['quantity'])
df['tipo_prd_id'] = pd.to_numeric(df['tipo_prd_id'])
df['post_id'] = pd.to_numeric(df['post_id'])
data = df.copy()

# titulo descripcion
data['titulo_descripcion'] = data.titulo + ' ' + data.descripcion

# notas de credito
data.sort_values(['post_id', 'titulo_descripcion', 'fecha'], inplace=True)
data.reset_index(drop=True, inplace=True)
for i in data.index:
    if (data.type[i] == 'pmi-credit-note') and (data.product_id[i] == data.product_id[i-1]) and (data.post_id[i] == data.post_id[i-1]) and (data.fecha[i] > data.fecha[i-1]):
        data.loc[i-1, 'quantity'] = data['quantity'][i-1] - data['quantity'][i]
data = data[data.type != 'pmi-credit-note']
data = data[data.quantity != 0]

# campo unidad
uni10_mask = data.titulo_descripcion.str.contains("10")
uni20_mask = data.titulo_descripcion.str.contains("20")
uni12_mask = data.titulo_descripcion.str.contains("12")
otros_mask = data.tipo_prd_id == 2
data.loc[uni10_mask == True, "Unidad"] = 10
data.loc[uni20_mask == True, "Unidad"] = 20
data.loc[uni12_mask == True, "Unidad"] = 12
data.loc[otros_mask == True, "Unidad"] = 1

# presentacion
Box_mask = data.titulo_descripcion.str.contains("Box|BOX|box")
soft_mask = data.titulo_descripcion.str.contains("Box|BOX|box") == False
otros_mask = data.tipo_prd_id == 2
data.Presentacion = "sin datos"
data.loc[Box_mask == True, "Presentacion"] = "Box"
data.loc[soft_mask == True, "Presentacion"] = "Soft_pack"
data.loc[otros_mask == True, "Presentacion"] = "Otros Productos"

# combos
cantidades_que_indican_combo = [i for i in data.quantity.unique() if (i < 10 or i % 2 != 0) and i != 15 and i != 45]
data['Combo'] = 0
for i in data.index:
    if data.loc[i]['quantity'] in cantidades_que_indican_combo:
        data.loc[i, 'Combo'] = 1
data['Combo'] = pd.to_numeric(data['Combo'])

# cartones por mes
data['fecha'] = pd.to_datetime(data.fecha)
data["Periodo"] = data["fecha"].apply(lambda x: x.strftime('%Y-%m'))
data["Carton_unidad"] = 0
data.loc[data["tipo_prd_id"] == 1, "Carton_unidad"] = 10
data.loc[data["titulo_descripcion"] == "Parliament Super Slims Box 20", "Carton_unidad"] = 15
data["Carton_cantidad"] = data["quantity"] / data["Carton_unidad"]

# marca y categoria
catalogacion = pd.read_csv('./Catalogación - Sheet1.csv').drop(columns=['Unnamed: 0'])
catalogacion.rename(columns={'PRODUCTO': 'titulo'}, inplace=True)
data['titulo'] = data.titulo.str.replace('amp;', '')
data['titulo'] = data.titulo.str.replace('\xa0', ' ')
data = data.merge(catalogacion, how='left')
data['MARCA'] = data['MARCA'].fillna('otros')

# precios
query = f"""
SELECT product_id, creation_date, neto 
FROM pr_2_pmi_prices_log 
WHERE product_id in {tuple(data.product_id.values)};
"""
cursor.execute(query)
resultado = cursor.fetchall()
precios = pd.DataFrame(resultado, columns=['product_id', 'fecha', 'precio'])
precios.sort_values(['product_id', 'fecha'], inplace=True)
precios.drop_duplicates(subset=['product_id'], keep='last', inplace=True)
precios.drop(columns=['fecha'], inplace=True)
data = data.merge(precios, how='left')
data['total_actualizado'] = data.precio * data.quantity
data.tail()

# csv mes anterior
data_anterior = pd.read_csv('/Users/pablojalil/Documents/Analytics Town/PMI/Mensual/Sin Clientes Historicos/2024-02/data_hasta_FEB_2024.csv', index_col=0)

# elaboro indice de actualizacion con los 4 productos más vendidos del mes
productos_mas_vendidos = data.groupby('product_id').sum(numeric_only=True)['quantity'].reset_index().sort_values('quantity', ascending=False).head(4)['product_id'].values
canasta_nueva = precios[precios.product_id.isin(productos_mas_vendidos)]['precio'].sum()
productos_viejos = data_anterior[data_anterior.product_id.isin(productos_mas_vendidos)]
productos_viejos['precio'] = productos_viejos['total_actualizado'] / productos_viejos['quantity']
productos_viejos = productos_viejos.groupby('product_id').sum(numeric_only=True).reset_index()
productos_viejos = productos_viejos[productos_viejos.product_id.isin(productos_mas_vendidos)]['precio'].sum()
indice_actualizacion = (canasta_nueva - productos_viejos) / productos_viejos

# normalizo precios por unidad de 1 carton
data['precio_unidad'] = data.precio / data['Carton_unidad']
data['precio_total'] = data['precio'] * data['quantity']
data['total_menor_unidad'] = data['precio_unidad'] * data['quantity']

# unidades vendidas por mes
venta_mensual = data.groupby(['Periodo', 'titulo']).agg({
    'total_actualizado': 'sum',
    'quantity': 'sum'
}).reset_index()

# unidades vendidas
venta_mensual['Total'] = venta_mensual['total_actualizado']
venta_mensual['unidades'] = venta_mensual['quantity']

# aplico clustering KMeans
scaler = StandardScaler()
X = scaler.fit_transform(venta_mensual[['Total', 'unidades']])
kmeans = KMeans(n_clusters=3, random_state=0).fit(X)
venta_mensual['cluster'] = kmeans.labels_

# crear gráficos
plt.figure(figsize=(10, 6))
sns.scatterplot(data=venta_mensual, x='Total', y='unidades', hue='cluster', palette='Set1', s=100, alpha=0.7)
plt.title('Segmentación de ventas por Total y Unidades')
plt.xlabel('Total')
plt.ylabel('Unidades')
plt.legend(title='Cluster')
plt.show()

# guardar el modelo
filename = 'kmeans_model.pkl'
with open(filename, 'wb') as file:
    pickle.dump(kmeans, file)

print(f"Modelo KMeans guardado en {filename}")

# cerrar conexión
cursor.close()
cnx.close()
