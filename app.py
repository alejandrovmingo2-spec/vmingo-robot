import streamlit as st
import pandas as pd
import PyPDF2
import re
import io
import zipfile
from datetime import datetime

st.set_page_config(page_title="Vmingo ERP - Robot Almacén", page_icon="🤖", layout="wide")

# =====================================================================
# FUNCIONES BÁSICAS Y LECTURA
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
        
        df_f['ORDER_ID'] = df[col_pedido] if col_pedido else "TEMU_SD"
        df_f['TRACKING_ID'] = df[col_track] if col_track else ""
        df_f['PEDIDO'] = df_f['ORDER_ID']
        df_f['SKU'] = df[col_sku] if col_sku else ""
        df_f['NOMBRE_ORIGINAL'] = df[col_nom] if col_nom else ""
        df_f['CANTIDAD'] = df[col_cant] if col_cant else 1
        
    elif plataforma == 'TIKTOK':
        col_order = cols_map.get('order id', cols_map.get('id de pedido'))
        col_track = cols_map.get('tracking id', cols_map.get('id de seguimiento'))
        col_sku = cols_map.get('seller sku', cols_map.get('sku del vendedor'))
        col_nom = cols_map.get('product name', cols_map.get('nombre del producto'))
        col_cant = cols_map.get('quantity', cols_map.get('cantidad'))
        
        df_f['ORDER_ID'] = df[col_order] if col_order else "TK_SD"
        df_f['TRACKING_ID'] = df[col_track] if col_track else ""
        # ANTI-FUSIÓN: Si hay Tracking (JMX), lo usa para separar cajas. Si no, usa Order ID.
        df_f['PEDIDO'] = df_f['TRACKING_ID'].replace('', pd.NA).fillna(df_f['ORDER_ID'])
        df_f['SKU'] = df[col_sku] if col_sku else ""
        df_f['NOMBRE_ORIGINAL'] = df[col_nom] if col_nom else ""
        df_f['CANTIDAD'] = df[col_cant] if col_cant else 1
        
    elif plataforma == 'SHEIN':
        col_pedido = cols_map.get('número de pedido', cols_map.get('numero de pedido'))
        col_track = cols_map.get('número de carta de porte de ida y vuelta', cols_map.get('numero de carta de porte de ida y vuelta'))
        col_sku = cols_map.get('sku del vendedor')
        col_nom = cols_map.get('nombre del producto')
        
        df_f['ORDER_ID'] = df[col_pedido] if col_pedido else "SH_SD"
        df_f['TRACKING_ID'] = df[col_track] if col_track else ""
        df_f['PEDIDO'] = df_f['ORDER_ID']
        df_f['SKU'] = df[col_sku] if col_sku else ""
        df_f['NOMBRE_ORIGINAL'] = df[col_nom] if col_nom else ""
        df_f['CANTIDAD'] = 1
        
    df_f['PLATAFORMA'] = plataforma
    df_f['ORDEN_ORIGINAL'] = range(len(df_f)) 
    df_f['PEDIDO'] = df_f['PEDIDO'].fillna('').astype(str).apply(lambda x: x.replace('.0', '') if x.endswith('.0') else x).str.strip()
    df_f['ORDER_ID'] = df_f['ORDER_ID'].fillna('').astype(str).str.strip()
    df_f['TRACKING_ID'] = df_f['TRACKING_ID'].fillna('').astype(str).str.strip()
    return df_f[df_f['PEDIDO'] != 'nan']

# =====================================================================
# EL CEREBRO DE REPARTICIÓN (CON OPCIÓN DE AVALANCHA)
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
    
    # 1. DETERMINAR CATEGORÍA
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
    
    # 2. REPARTICIÓN POR TIENDA
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
        
        # Normales (Agrupar SKUs iguales para 1 sola persona)
        df_mini_norm = df_car[(df_car['TIPOS_PRODUCTO'] == 1) & (df_car['TIPO'] != 'CAJA')]
        sku_vol_norm = df_mini_norm.groupby('SKU')['CANTIDAD'].sum().sort_values(ascending=False).index.tolist()
        for sku in sku_vol_norm:
            peds_sku = df_mini_norm[df_mini_norm['SKU'] == sku]['PEDIDO'].unique().tolist()
            if plat == 'SHEIN': peds_sku = df_plat[df_plat['PEDIDO'].isin(peds_sku)].sort_values('ORDEN_ORIGINAL')['PEDIDO'].unique().tolist()
            else: peds_sku.sort()
            emp_asignado = empleados[emp_idx % num_emp] 
            for p in peds_sku: asignaciones[p] = emp_asignado
            emp_idx += 1

        # Cajas (Tope de 15)
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

        # Mixtos
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
# TABS INTERFAZ
# =====================================================================
st.title("🤖 Vmingo ERP: Coordinador Maestro")

tab1, tab2 = st.tabs(["🛒 FASE 1: Picking (CSVs)", "📦 FASE 2: Empaque (Guías PDF)"])

# ----------------- FASE 1 -----------------
with tab1:
    st.markdown("### 1. Surtido de Almacén")
    col_t, col_s, col_k = st.columns(3)
    with col_t: f_t = st.file_uploader("CSV TEMU", type=["csv"], key="t1")
    with col_s: f_s = st.file_uploader("CSV SHEIN", type=["csv"], key="s1")
    with col_k: f_k = st.file_uploader("CSV TIKTOK", type=["csv"], key="k1")
    
    col_b, col_e = st.columns([1, 2])
    with col_b: f_base = st.file_uploader("BASE", type=["xlsx", "xlsm"], key="b1")
    with col_e: e_in = st.text_input("Equipo en Turno:", "ANTONIO, IVAN, CRISTIAN, ALEXIS, OSCAR")

    # --- DETECTOR < 600 ---
    if f_t or f_s or f_k:
        temp_dfs = []
        if f_t: temp_dfs.append(procesar_csv(f_t, detectar_plataforma_csv(f_t)[1], 'TEMU'))
        if f_s: temp_dfs.append(procesar_csv(f_s, detectar_plataforma_csv(f_s)[1], 'SHEIN'))
        if f_k: temp_dfs.append(procesar_csv(f_k, detectar_plataforma_csv(f_k)[1], 'TIKTOK'))
        
        total_peds = pd.concat(temp_dfs)['PEDIDO'].nunique() if temp_dfs else 0
        
        if total_peds < 600:
            st.warning(f"⚠️ Detectamos solo **{total_peds}** pedidos totales en los CSVs.")
            usar_ava = st.radio("¿Deseas activar la logística de AVALANCHA?", ["SÍ", "NO"], index=1)
        else:
            usar_ava = "SÍ"
            st.info(f"📊 Total de pedidos: **{total_peds}**. Aplicando Avalancha automática.")

        if st.button("📊 Generar Picking", type="primary"):
            raw_emps = [e.strip().upper() for e in e_in.split(',') if e.strip()]
            emps = list(dict.fromkeys(raw_emps)) 
            
            with st.spinner("Procesando asignaciones..."):
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
                        # Resumen Top 5 Avalancha
                        df_resumen = df_final[df_final['CATEGORIA'] == 'AVALANCHA'].groupby(['SKU','Nombre Correcto']).agg(CANTIDAD=('CANTIDAD', 'sum'), TIENE_TEMU=('PLATAFORMA', lambda x: 'TEMU' in x.values)).reset_index()
                        df_resumen['Aviso'] = df_resumen.apply(lambda r: '🟢 LLEVA TEMU' if r['TIENE_TEMU'] else '', axis=1)
                        df_resumen[['SKU', 'Nombre Correcto', 'CANTIDAD', 'Aviso']].sort_values(by='CANTIDAD', ascending=False).to_excel(writer, sheet_name='🔥 TOP 5 AVALANCHA', index=False)
                        
                        # Asignación detallada Avalancha (CON TRACKING ID)
                        df_asig_ava = df_final[df_final['CATEGORIA'] == 'AVALANCHA'].copy()
                        df_asig_ava = df_asig_ava[['ASIGNADO_A', 'PLATAFORMA', 'ORDER_ID', 'TRACKING_ID', 'SKU', 'Nombre Correcto', 'CANTIDAD']]
                        df_asig_ava.rename(columns={'ASIGNADO_A': 'EMPLEADO'}, inplace=True)
                        df_asig_ava = df_asig_ava.sort_values(by=['EMPLEADO', 'Nombre Correcto'])
                        df_asig_ava.to_excel(writer, sheet_name='⚡ ASIGNACION AVALANCHA', index=False)
                        ws_asig = writer.sheets['⚡ ASIGNACION AVALANCHA']
                        ws_asig.set_column('A:A', 15); ws_asig.set_column('C:D', 20); ws_asig.set_column('F:F', 50)
                    
                    for i, e in enumerate(emps):
                        df_e = df_final[df_final['ASIGNADO_A'] == e].copy()
                        if not df_e.empty:
                            # 1. Hoja de DETALLES por empleado (CON TRACKING ID para que no se pierdan)
                            df_e[['PLATAFORMA', 'ORDER_ID', 'TRACKING_ID', 'SKU', 'Nombre Correcto', 'CANTIDAD', 'CATEGORIA']].to_excel(writer, sheet_name=f"{e}_Detalles", index=False)
                            
                            # 2. Ticket Térmico Agrupado (Solo carritos)
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
                                encabezados = ['NO', 'SKU', 'NOMBRE COMUN', 'CANTI\nDAD']
                                for col, encab in enumerate(encabezados):
                                    if encab == 'CANTI\nDAD': hoja_ticket.write(3, col, encab, fmt_wrap)
                                    else: hoja_ticket.write(3, col, encab, fmt_header)
                                total_piezas_emp = 0
                                fila = 4
                                for idx, item in picking_list.iterrows():
                                    cant = int(item['CANTIDAD'])
                                    total_piezas_emp += cant
                                    nom = f"🟢 [TEMU - AVISAR] {item['Nombre Correcto']}" if item['TIENE_TEMU'] else item['Nombre Correcto']
                                    hoja_ticket.write(fila, 0, idx + 1, fmt_td_centro) 
                                    hoja_ticket.write(fila, 1, item['SKU'], fmt_td_centro)            
                                    hoja_ticket.write(fila, 2, nom, fmt_td_izq)  
                                    hoja_ticket.write(fila, 3, cant, fmt_td_centro)     
                                    fila += 1
                                hoja_ticket.write(fila, 0, len(picking_list) + 1, fmt_td_centro)
                                hoja_ticket.merge_range(fila, 1, fila, 2, 'Total de Carrito', fmt_total)
                                hoja_ticket.write(fila, 3, total_piezas_emp, fmt_total)
                                hoja_ticket.set_column('A:A', 4); hoja_ticket.set_column('B:B', 16); hoja_ticket.set_column('C:C', 38); hoja_ticket.set_column('D:D', 6)
                
                st.success("✅ ¡Picking listo con Detalles y Tracking IDs incluidos!")
                st.download_button("📥 Descargar Excel Picking", output.getvalue(), f"Picking_Termico_{datetime.now().strftime('%d-%m-%Y')}.xlsx", "application/vnd.ms-excel")

# ----------------- FASE 2 -----------------
with tab2:
    st.markdown("### 2. Generador de Guías (Buscador Original)")
    col_t2, col_s2, col_k2 = st.columns(3)
    with col_t2:
        csv_t2 = st.file_uploader("CSV Temu (F2)", type=["csv"], key="ct2")
        pdf_t2 = st.file_uploader("PDF Temu", type=["pdf"], key="pt2")
    with col_s2:
        csv_s2 = st.file_uploader("CSV Shein (F2)", type=["csv"], key="cs2")
        pdf_s2 = st.file_uploader("PDF Shein", type=["pdf"], key="ps2")
    with col_k2:
        csv_k2 = st.file_uploader("CSV TikTok (F2)", type=["csv"], key="ck2")
        csv_jmx = st.file_uploader("CSV TikTok JMX", type=["csv"], key="cjmx")
        pdf_k2 = st.file_uploader("PDF TikTok", type=["pdf"], key="pk2")
    
    f_base2 = st.file_uploader("BASE (F2)", type=["xlsx", "xlsm"], key="b2")

    if st.button("✂️ Cortar Guías", type="primary"):
        csvs2 = [f for f in [csv_t2, csv_s2, csv_k2] if f is not None]
        raw_emps2 = [e.strip().upper() for e in e_in.split(',') if e.strip()]
        emps2 = list(dict.fromkeys(raw_emps2))
        
        with st.spinner("Cortando..."):
            dicc_nom2, dicc_tipo2 = {}, {}
            if f_base2:
                try: df_b2 = pd.read_excel(f_base2, sheet_name='BASE', dtype=str)
                except: df_b2 = pd.read_excel(f_base2, dtype=str)
                df_b2.columns = df_b2.columns.str.strip().str.upper()
                for _, r in df_b2.iterrows():
                    s = str(r.get('SKU','')).strip()
                    if s: dicc_nom2[s] = str(r.get('NOMBRE PLATAFORMA','')).strip(); dicc_tipo2[s] = str(r.get('TIPO','NORMAL')).strip().upper()

            processed_dfs = [procesar_csv(a, detectar_plataforma_csv(a)[1], detectar_plataforma_csv(a)[0]) for a in csvs2]
            
            tot_f2 = pd.concat(processed_dfs)['PEDIDO'].nunique() if processed_dfs else 0
            df_matriz = unificar_y_distribuir(processed_dfs, emps2, dicc_nom2, dicc_tipo2, activar_avalancha=(tot_f2>=600))
            
            # MAPEO INTELIGENTE DE JMX Y ORDER ID
            mapa_pedidos_tiktok = {}
            for _, r in df_matriz[df_matriz['PLATAFORMA'] == 'TIKTOK'].iterrows():
                ped_final = str(r['PEDIDO']).strip() # Puede ser el JMX o el Order ID
                ord_id = str(r['ORDER_ID']).strip()
                trk_id = str(r['TRACKING_ID']).strip()
                if ord_id: mapa_pedidos_tiktok[ord_id] = ped_final
                if trk_id: mapa_pedidos_tiktok[trk_id] = ped_final
            
            if csv_jmx:
                df_jmx = procesar_csv(csv_jmx, detectar_plataforma_csv(csv_jmx)[1], 'TIKTOK')
                for _, r in df_jmx.iterrows():
                    ord_id = str(r['ORDER_ID']).strip()
                    trk_id = str(r['TRACKING_ID']).strip()
                    if ord_id and trk_id: 
                        mapa_pedidos_tiktok[trk_id] = ord_id

            paginas_por_pedido = {}
            stats = {"TEMU": 0, "SHEIN": 0, "TIKTOK": 0}

            # TU CÓDIGO INTACTO: TIKTOK
            if pdf_k2: 
                reader = PyPDF2.PdfReader(pdf_k2)
                cur_tk = None
                tmp = {}
                for p in reader.pages:
                    m = re.findall(r'(JMX\d+)', p.extract_text() or "")
                    if m: cur_tk = str(m[0]).strip(); tmp[cur_tk] = []
                    if cur_tk: tmp[cur_tk].append(p)
                for k, v in tmp.items():
                    # Aquí rescata el faltante, mapeando el JMX al Order ID final
                    real = mapa_pedidos_tiktok.get(k, k)
                    paginas_por_pedido[real] = v
                    stats["TIKTOK"] += 1

            # TU CÓDIGO INTACTO: TEMU
            if pdf_t2:
                reader = PyPDF2.PdfReader(pdf_t2)
                cur_te = None
                for i, p in enumerate(reader.pages):
                    m = re.findall(r'(PO-\d{3}-\d+)', p.extract_text() or "")
                    if m: 
                        cur_te = str(m[0]).strip(); paginas_por_pedido[cur_te] = []
                        if i > 0: paginas_por_pedido[cur_te].append(reader.pages[i-1])
                        stats["TEMU"] += 1
                    if cur_te: paginas_por_pedido[cur_te].append(p)

            # TU CÓDIGO INTACTO: SHEIN
            if pdf_s2:
                reader = PyPDF2.PdfReader(pdf_s2)
                chunks, cur_sh = [], []
                for p in reader.pages:
                    txt = p.extract_text() or ""
                    if re.search(r'(JMX|GSH|J&T|TODOOR|D2D)', txt.upper()) and 'DECLARACIÓN' not in txt.upper():
                        if cur_sh: chunks.append(cur_sh)
                        cur_sh = [p]
                    else: cur_sh.append(p)
                if cur_sh: chunks.append(cur_sh)
                peds_s = df_matriz[df_matriz['PLATAFORMA'] == 'SHEIN'].sort_values('ORDEN_ORIGINAL')['PEDIDO'].unique()
                for i, ped in enumerate(peds_s):
                    if i < len(chunks): paginas_por_pedido[ped] = chunks[i]; stats["SHEIN"] += 1

            # --- RENDER DE MÉTRICAS FASE 2 ---
            st.subheader("📊 Diagnóstico de Guías Cortadas (Fase 2)")
            m1, m2, m3, m4 = st.columns(4)
            m1.metric("🔴 TEMU", stats["TEMU"])
            m2.metric("🟢 SHEIN", stats["SHEIN"])
            m3.metric("🔵 TIKTOK", stats["TIKTOK"])
            m4.metric("🏆 TOTAL CORTADO", sum(stats.values()))

            zip_buf = io.BytesIO()
            with zipfile.ZipFile(zip_buf, "a", zipfile.ZIP_DEFLATED, False) as zf:
                excel_buf = io.BytesIO()
                with pd.ExcelWriter(excel_buf, engine='xlsxwriter') as writer:
                    df_sin_pdf = df_matriz[~df_matriz['PEDIDO'].isin(paginas_por_pedido.keys())].copy()
                    if not df_sin_pdf.empty:
                        df_sin_pdf[['PEDIDO', 'PLATAFORMA', 'ORDER_ID', 'TRACKING_ID', 'SKU', 'Nombre Correcto']].drop_duplicates(subset=['PEDIDO']).to_excel(writer, sheet_name='🚨 FALTAN EN PDF', index=False)
                    
                    if tot_f2 >= 600:
                        df_matriz[df_matriz['CATEGORIA']=='AVALANCHA'][['ASIGNADO_A','PLATAFORMA','ORDER_ID','TRACKING_ID','SKU','Nombre Correcto']].to_excel(writer, sheet_name='⚡ AVALANCHA GUIAS', index=False)
                    
                    for i, e in enumerate(emps2):
                        df_e = df_matriz[df_matriz['ASIGNADO_A'] == e].copy()
                        df_e = df_e[df_e['PEDIDO'].isin(paginas_por_pedido.keys())]
                        if not df_e.empty:
                            df_e[['PLATAFORMA','ORDER_ID','TRACKING_ID','SKU','Nombre Correcto','CANTIDAD', 'CATEGORIA']].to_excel(writer, sheet_name=f"{e}_Detalles", index=False)
                            df_tkt = df_e[df_e['CATEGORIA'] == 'CARRITO'].copy()
                            df_ava_pdf = df_e[df_e['CATEGORIA'] == 'AVALANCHA'].copy()
                            if not df_tkt.empty:
                                pw = PyPDF2.PdfWriter()
                                for p in df_tkt['PEDIDO'].unique():
                                    for pag in paginas_por_pedido[p]: pw.add_page(pag)
                                buf = io.BytesIO(); pw.write(buf); zf.writestr(f"2_CARRITO_{e}.pdf", buf.getvalue())
                            if not df_ava_pdf.empty:
                                pw = PyPDF2.PdfWriter()
                                for p in df_ava_pdf['PEDIDO'].unique():
                                    for pag in paginas_por_pedido[p]: pw.add_page(pag)
                                buf = io.BytesIO(); pw.write(buf); zf.writestr(f"1_AVALANCHA_{e}.pdf", buf.getvalue())
                zf.writestr(f"Auditoria_Tickets.xlsx", excel_buf.getvalue())
            st.download_button("📦 Descargar ZIP Guías", zip_buf.getvalue(), f"Empaque_Vmingo_{datetime.now().strftime('%d-%m-%Y')}.zip", "application/zip")
