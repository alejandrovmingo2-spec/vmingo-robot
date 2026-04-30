import streamlit as st
import pandas as pd
import io
from datetime import datetime

st.set_page_config(page_title="Paso 1 - Fase de Picking", page_icon="🛒", layout="wide")

# =====================================================================
# FUNCIONES BÁSICAS DE LECTURA (TU CÓDIGO BASE)
# =====================================================================
def limpiar_nombre(texto):
    idx = texto.lower().find('detalle')
    if idx != -1: return texto[:idx].strip()
    return texto.strip()

def detectar_plataforma_csv(archivo_csv_buffer):
    encodings_a_probar = ['utf-8-sig', 'utf-8', 'latin1', 'cp1252']
    contenido = archivo_csv_buffer.getvalue()
    for cod in encodings_a_probar:
        try:
            texto = contenido.decode(cod)
            lineas = texto.splitlines()
            for linea in lineas[:5]:
                lin_low = linea.lower()
                if 'id del pedido' in lin_low and 'sku de contribución' in lin_low: return 'TEMU', cod
                if ('order id' in lin_low or 'id de pedido' in lin_low) and ('seller sku' in lin_low or 'sku del vendedor' in lin_low): return 'TIKTOK', cod
                if 'número de pedido' in lin_low and 'sku del vendedor' in lin_low: return 'SHEIN', cod
        except: pass
    return 'DESCONOCIDA', None

def procesar_csv(archivo, codificacion, plataforma):
    archivo.seek(0)
    texto_csv = archivo.getvalue().decode(codificacion)
    skip_lineas = 0
    for i, linea in enumerate(texto_csv.splitlines()):
        lin_low = linea.lower()
        if (plataforma == 'TEMU' and 'id del pedido' in lin_low) or \
           (plataforma == 'TIKTOK' and ('order id' in lin_low or 'id de pedido' in lin_low)) or \
           (plataforma == 'SHEIN' and 'número de pedido' in lin_low):
            skip_lineas = i; break
    archivo.seek(0) 
    
    df = pd.read_csv(archivo, skiprows=skip_lineas, encoding=codificacion, dtype=str)
    df = df.dropna(how='all')
    cols_map = {c.lower().strip(): c for c in df.columns}
    df_f = pd.DataFrame()

    if plataforma == 'TEMU':
        col_pedido = cols_map.get('id del pedido')
        col_sku = cols_map.get('sku de contribución', cols_map.get('sku de contribucion'))
        col_nom = cols_map.get('nombre del producto')
        col_cant = cols_map.get('cantidad a enviar')
        col_track = cols_map.get('número de seguimiento', cols_map.get('numero de seguimiento'))
        
        df_f['ORDER_ID'] = df[col_pedido]
        df_f['TRACKING_ID'] = df[col_track] if col_track else ""
        df_f['PEDIDO'] = df_f['ORDER_ID']
        df_f['SKU'] = df[col_sku]
        df_f['NOMBRE_ORIGINAL'] = df[col_nom]
        df_f['CANTIDAD'] = df[col_cant]
        
    elif plataforma == 'TIKTOK':
        col_order = cols_map.get('order id', cols_map.get('id de pedido'))
        col_track = cols_map.get('tracking id', cols_map.get('id de seguimiento'))
        col_sku = cols_map.get('seller sku', cols_map.get('sku del vendedor'))
        col_nom = cols_map.get('product name', cols_map.get('nombre del producto'))
        col_cant = cols_map.get('quantity', cols_map.get('cantidad'))
        
        df_f['ORDER_ID'] = df[col_order]
        df_f['TRACKING_ID'] = df[col_track] if col_track else ""
        # Fase 1: Usamos Order ID porque JMX a veces no viene en la mañana
        df_f['PEDIDO'] = df_f['ORDER_ID'] 
        df_f['SKU'] = df[col_sku]
        df_f['NOMBRE_ORIGINAL'] = df[col_nom]
        df_f['CANTIDAD'] = df[col_cant]
        
    elif plataforma == 'SHEIN':
        col_pedido = cols_map.get('número de pedido', cols_map.get('numero de pedido'))
        col_track = cols_map.get('número de carta de porte de ida y vuelta', cols_map.get('numero de carta de porte de ida y vuelta'))
        col_sku = cols_map.get('sku del vendedor')
        col_nom = cols_map.get('nombre del producto')
        
        df_f['ORDER_ID'] = df[col_pedido]
        df_f['TRACKING_ID'] = df[col_track] if col_track else ""
        df_f['PEDIDO'] = df_f['ORDER_ID']
        df_f['SKU'] = df[col_sku]
        df_f['NOMBRE_ORIGINAL'] = df[col_nom]
        df_f['CANTIDAD'] = 1
        
    df_f['PLATAFORMA'] = plataforma
    df_f['ORDEN_ORIGINAL'] = range(len(df_f)) 
    df_f['PEDIDO'] = df_f['PEDIDO'].fillna('').astype(str).apply(lambda x: x.replace('.0', '') if x.endswith('.0') else x).str.strip()
    df_f['ORDER_ID'] = df_f['ORDER_ID'].fillna('').astype(str).apply(lambda x: x.replace('.0', '') if x.endswith('.0') else x).str.strip()
    df_f['TRACKING_ID'] = df_f['TRACKING_ID'].fillna('').astype(str).apply(lambda x: x.replace('.0', '') if x.endswith('.0') else x).str.strip()
    return df_f[df_f['PEDIDO'] != 'nan']

# =====================================================================
# EL CEREBRO DE REPARTICIÓN
# =====================================================================
def unificar_y_distribuir(dataframes, empleados, dicc_nombres, dicc_tipos, activar_avalancha=True):
    df_total = pd.concat(dataframes, ignore_index=True)
    df_total['SKU'] = df_total['SKU'].astype(str).str.strip()
    df_total['CANTIDAD'] = pd.to_numeric(df_total['CANTIDAD'], errors='coerce').fillna(1)
    
    df_total['Nombre Correcto'] = df_total['SKU'].apply(lambda x: limpiar_nombre(dicc_nombres.get(x, "SIN NOMBRE EN BASE")))
    df_total['TIPO'] = df_total['SKU'].apply(lambda x: dicc_tipos.get(x, "NORMAL"))
    
    conteo_pedidos = df_total.groupby('PEDIDO')['SKU'].nunique().reset_index()
    conteo_pedidos.columns = ['PEDIDO', 'TIPOS_PRODUCTO']
    df_total = df_total.merge(conteo_pedidos, on='PEDIDO')
    
    if activar_avalancha:
        df_single = df_total[df_total['TIPOS_PRODUCTO'] == 1]
        top_5_skus = df_single.groupby('SKU')['CANTIDAD'].sum().nlargest(5).index.tolist()
        df_total['CATEGORIA'] = df_total.apply(
            lambda r: 'AVALANCHA' if (r['TIPOS_PRODUCTO'] == 1 and r['SKU'] in top_5_skus) else 'CARRITO', axis=1
        )
    else:
        df_total['CATEGORIA'] = 'CARRITO'
        top_5_skus = []
    
    asignaciones = {}
    emp_idx = 0
    num_emp = len(empleados)
    
    # REPARTICIÓN AISLADA POR TIENDA
    for plat in ['TIKTOK', 'SHEIN', 'TEMU']:
        df_plat = df_total[df_total['PLATAFORMA'] == plat].copy()
        if df_plat.empty: continue
        
        # A) Avalancha
        if activar_avalancha:
            df_ava = df_plat[df_plat['CATEGORIA'] == 'AVALANCHA']
            pedidos_ava = df_ava['PEDIDO'].unique().tolist()
            if plat == 'SHEIN': pedidos_ava = df_plat[df_plat['PEDIDO'].isin(pedidos_ava)].sort_values('ORDEN_ORIGINAL')['PEDIDO'].unique().tolist()
            else: pedidos_ava.sort()
            for p in pedidos_ava:
                asignaciones[p] = empleados[emp_idx % num_emp]
                emp_idx += 1

        # B) Carritos
        df_car = df_plat[df_plat['CATEGORIA'] == 'CARRITO']
        
        df_mini_norm = df_car[(df_car['TIPOS_PRODUCTO'] == 1) & (df_car['TIPO'] != 'CAJA')]
        sku_vol_norm = df_mini_norm.groupby('SKU')['CANTIDAD'].sum().sort_values(ascending=False).index.tolist()
        for sku in sku_vol_norm:
            peds_sku = df_mini_norm[df_mini_norm['SKU'] == sku]['PEDIDO'].unique().tolist()
            if plat == 'SHEIN': peds_sku = df_plat[df_plat['PEDIDO'].isin(peds_sku)].sort_values('ORDEN_ORIGINAL')['PEDIDO'].unique().tolist()
            else: peds_sku.sort()
            emp_asignado = empleados[emp_idx % num_emp] 
            for p in peds_sku: asignaciones[p] = emp_asignado
            emp_idx += 1

        limite_cajas = 15
        df_mini_caja = df_car[(df_car['TIPOS_PRODUCTO'] == 1) & (df_car['TIPO'] == 'CAJA')]
        sku_vol_caja = df_mini_caja.groupby('SKU')['CANTIDAD'].sum().sort_values(ascending=False).index.tolist()
        for sku in sku_vol_caja:
            peds_sku = df_mini_caja[df_mini_caja['SKU'] == sku]['PEDIDO'].unique().tolist()
            if plat == 'SHEIN': peds_sku = df_plat[df_plat['PEDIDO'].isin(peds_sku)].sort_values('ORDEN_ORIGINAL')['PEDIDO'].unique().tolist()
            else: peds_sku.sort()
            for i in range(0, len(peds_sku), limite_cajas):
                chunk = peds_sku[i : i + limite_cajas]
                emp_asignado = empleados[emp_idx % num_emp]
                for p in chunk: asignaciones[p] = emp_asignado
                emp_idx += 1

        df_mixtos = df_car[df_car['TIPOS_PRODUCTO'] > 1]
        pedidos_mixtos = df_mixtos['PEDIDO'].unique().tolist()
        if plat == 'SHEIN': 
            pedidos_mixtos = df_plat[df_plat['PEDIDO'].isin(pedidos_mixtos)].sort_values('ORDEN_ORIGINAL')['PEDIDO'].unique().tolist()
        else: 
            cant_mix = df_mixtos.groupby('PEDIDO')['CANTIDAD'].sum().to_dict()
            pedidos_mixtos.sort(key=lambda x: cant_mix.get(x, 0), reverse=True)
        for p in pedidos_mixtos:
            asignaciones[p] = empleados[emp_idx % num_emp]
            emp_idx += 1
                
    df_total['ASIGNADO_A'] = df_total['PEDIDO'].map(asignaciones)
    return df_total

# =====================================================================
# INTERFAZ FASE 1
# =====================================================================
st.title("🛒 FASE 1: Surtido de Almacén (Picking)")
col_t, col_s, col_k = st.columns(3)
with col_t: f_t = st.file_uploader("CSV TEMU", type=["csv"])
with col_s: f_s = st.file_uploader("CSV SHEIN", type=["csv"])
with col_k: f_k = st.file_uploader("CSV TIKTOK", type=["csv"])

col_b, col_e = st.columns([1, 2])
with col_b: f_base = st.file_uploader("BASE (Conumna TIPO)", type=["xlsx", "xlsm"])
with col_e: e_in = st.text_input("Equipo en Turno:", "ANTONIO, IVAN, CRISTIAN, ALEXIS, OSCAR")

if f_t or f_s or f_k:
    temp_dfs = []
    if f_t: temp_dfs.append(procesar_csv(f_t, detectar_plataforma_csv(f_t)[1], 'TEMU'))
    if f_s: temp_dfs.append(procesar_csv(f_s, detectar_plataforma_csv(f_s)[1], 'SHEIN'))
    if f_k: temp_dfs.append(procesar_csv(f_k, detectar_plataforma_csv(f_k)[1], 'TIKTOK'))
    
    total_peds = pd.concat(temp_dfs)['PEDIDO'].nunique() if temp_dfs else 0
    
    if total_peds < 600:
        st.warning(f"⚠️ Detectamos {total_peds} pedidos (Menos de 600).")
        usar_ava = st.radio("¿Deseas activar AVALANCHA?", ["SÍ", "NO"], index=1)
    else:
        usar_ava = "SÍ"
        st.info(f"📊 {total_peds} pedidos detectados. Avalancha activada.")

    if st.button("📊 Generar Documento de Almacén", type="primary"):
        raw_emps = [e.strip().upper() for e in e_in.split(',') if e.strip()]
        emps = list(dict.fromkeys(raw_emps)) 
        
        with st.spinner("Construyendo excel..."):
            dicc_nom, dicc_tipo = {}, {}
            if f_base:
                try: df_b = pd.read_excel(f_base, sheet_name='BASE', dtype=str)
                except: df_b = pd.read_excel(f_base, dtype=str)
                df_b.columns = df_b.columns.str.strip().str.upper()
                for _, r in df_b.iterrows():
                    s = str(r.get('SKU','')).strip()
                    if s:
                        dicc_nom[s] = str(r.get('NOMBRE PLATAFORMA','')).strip()
                        dicc_tipo[s] = str(r.get('TIPO','NORMAL')).strip().upper()

            df_final = unificar_y_distribuir(temp_dfs, emps, dicc_nom, dicc_tipo, activar_avalancha=(usar_ava=="SÍ"))
            
            output = io.BytesIO()
            colores_division = ['#FFD966', '#A9D08E', '#9BC2E6', '#F4B084', '#B4A7D6', '#93CDDD']
            
            with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
                if usar_ava == "SÍ":
                    df_resumen = df_final[df_final['CATEGORIA'] == 'AVALANCHA'].groupby(['SKU','Nombre Correcto']).agg(CANTIDAD=('CANTIDAD', 'sum'), TIENE_TEMU=('PLATAFORMA', lambda x: 'TEMU' in x.values)).reset_index()
                    df_resumen['Aviso'] = df_resumen.apply(lambda r: '🟢 LLEVA TEMU' if r['TIENE_TEMU'] else '', axis=1)
                    df_resumen[['SKU', 'Nombre Correcto', 'CANTIDAD', 'Aviso']].sort_values(by='CANTIDAD', ascending=False).to_excel(writer, sheet_name='🔥 TOP 5 AVALANCHA', index=False)
                    
                    df_asig_ava = df_final[df_final['CATEGORIA'] == 'AVALANCHA'][['ASIGNADO_A', 'PLATAFORMA', 'ORDER_ID', 'TRACKING_ID', 'SKU', 'Nombre Correcto', 'CANTIDAD']]
                    df_asig_ava.rename(columns={'ASIGNADO_A': 'EMPLEADO'}, inplace=True)
                    df_asig_ava = df_asig_ava.sort_values(by=['EMPLEADO', 'Nombre Correcto'])
                    df_asig_ava.to_excel(writer, sheet_name='⚡ ASIGNACION AVALANCHA', index=False)
                
                for i, e in enumerate(emps):
                    df_e = df_final[df_final['ASIGNADO_A'] == e].copy()
                    if not df_e.empty:
                        # 1. Pestaña Detalles (Con IDs)
                        df_e[['PLATAFORMA', 'ORDER_ID', 'TRACKING_ID', 'SKU', 'Nombre Correcto', 'CANTIDAD', 'CATEGORIA']].to_excel(writer, sheet_name=f"{e}_Detalles", index=False)
                        
                        # 2. Pestaña Ticket (Resumido para almacén)
                        df_tkt = df_e[df_e['CATEGORIA'] == 'CARRITO'].copy()
                        if not df_tkt.empty:
                            picking_list = df_tkt.groupby(['SKU', 'Nombre Correcto']).agg(CANTIDAD=('CANTIDAD', 'sum'), TIENE_TEMU=('PLATAFORMA', lambda x: 'TEMU' in x.values)).reset_index()
                            picking_list = picking_list.sort_values(by='Nombre Correcto').reset_index(drop=True)
                            
                            color_actual = colores_division[i % len(colores_division)]
                            hoja_ticket = writer.book.add_worksheet(f"🛒 {e}_Ticket")
                            fmt_header = writer.book.add_format({'bold': True, 'align': 'center', 'valign': 'vcenter', 'bg_color': color_actual, 'border': 1})
                            fmt_titulo_ticket = writer.book.add_format({'bold': True, 'font_size': 14, 'align': 'center', 'valign': 'vcenter', 'bg_color': color_actual, 'border': 1})
                            fmt_td_centro = writer.book.add_format({'border': 1, 'align': 'center', 'valign': 'vcenter', 'text_wrap': True})
                            fmt_td_izq = writer.book.add_format({'border': 1, 'align': 'left', 'valign': 'vcenter', 'text_wrap': True})
                            fmt_total = writer.book.add_format({'bold': True, 'border': 1, 'align': 'center', 'valign': 'vcenter', 'bg_color': '#D9D9D9'})
                            fmt_wrap = writer.book.add_format({'bold': True, 'align': 'center', 'valign': 'vcenter', 'text_wrap': True, 'bg_color': color_actual, 'border': 1})
                            
                            hoja_ticket.write('A1', f'DIVISION {i+1}', fmt_header)
                            hoja_ticket.write('D1', 'CARRITO', fmt_header) 
                            hoja_ticket.merge_range('A2:D2', f"SURTIR: {e.upper()}", fmt_titulo_ticket)
                            for col, enc in enumerate(['NO', 'SKU', 'NOMBRE COMUN', 'CANTI\nDAD']):
                                if enc == 'CANTI\nDAD': hoja_ticket.write(3, col, enc, fmt_wrap)
                                else: hoja_ticket.write(3, col, enc, fmt_header)
                            
                            total_p = 0
                            for row_idx, item in picking_list.iterrows():
                                cant = int(item['CANTIDAD']); total_p += cant
                                nom = f"🟢 [TEMU - AVISAR] {item['Nombre Correcto']}" if item['TIENE_TEMU'] else item['Nombre Correcto']
                                hoja_ticket.write(row_idx + 4, 0, row_idx + 1, fmt_td_centro) 
                                hoja_ticket.write(row_idx + 4, 1, item['SKU'], fmt_td_centro)            
                                hoja_ticket.write(row_idx + 4, 2, nom, fmt_td_izq)  
                                hoja_ticket.write(row_idx + 4, 3, cant, fmt_td_centro)     
                            hoja_ticket.write(len(picking_list) + 4, 0, len(picking_list) + 1, fmt_td_centro)
                            hoja_ticket.merge_range(len(picking_list) + 4, 1, len(picking_list) + 4, 2, 'Total de Carrito', fmt_total)
                            hoja_ticket.write(len(picking_list) + 4, 3, total_p, fmt_total)
                            hoja_ticket.set_column('A:A', 4); hoja_ticket.set_column('B:B', 16); hoja_ticket.set_column('C:C', 38); hoja_ticket.set_column('D:D', 6)
            
            st.success("✅ ¡Excel de Fase 1 generado con éxito!")
            st.download_button("📥 Descargar Picking Almacén", output.getvalue(), f"Picking_Termico_{datetime.now().strftime('%d-%m-%Y')}.xlsx", "application/vnd.ms-excel")
