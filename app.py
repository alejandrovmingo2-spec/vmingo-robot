import streamlit as st
import pandas as pd
import PyPDF2
import re
import io
import zipfile

st.set_page_config(page_title="Robot Multiplataforma Vmingo", page_icon="🤖", layout="centered")

def limpiar_nombre(texto):
    idx = texto.lower().find('detalle')
    if idx != -1:
        return texto[:idx].strip()
    return texto.strip()

def detectar_plataforma_web(archivo_csv_buffer):
    encodings_a_probar = ['utf-8-sig', 'utf-8', 'latin1', 'cp1252']
    contenido = archivo_csv_buffer.getvalue()
    for cod in encodings_a_probar:
        try:
            texto = contenido.decode(cod)
            lineas = texto.splitlines()
            for linea in lineas[:5]:
                if 'ID del pedido' in linea and 'sku de contribución' in linea:
                    return 'TEMU', cod
                if 'Order ID' in linea and 'Seller SKU' in linea:
                    return 'TIKTOK', cod
                if 'Número de pedido' in linea and 'SKU del vendedor' in linea:
                    return 'SHEIN', cod
        except:
            pass
    return 'DESCONOCIDA', None

st.title("🤖 Robot Multiplataforma Vmingo")
st.markdown("Sube tus documentos para repartir las guías de forma equitativa.")

# --- INTERFAZ DE USUARIO ---
col1, col2 = st.columns(2)
with col1:
    archivo_csv = st.file_uploader("1. Sube el CSV (Temu / TikTok / Shein)", type=["csv"])
    archivo_pdf = st.file_uploader("2. Sube el PDF gigante", type=["pdf"])
with col2:
    archivo_base = st.file_uploader("3. Sube tu BASE (Opcional)", type=["xlsx", "xlsm"])

empleados_input = st.text_input("Nombres del equipo en turno (separados por coma):", "ALDO, BELEN, ALDRICH")

if st.button("🚀 Procesar Guías", type="primary"):
    if not archivo_csv or not archivo_pdf:
        st.error("❌ Error: Faltan archivos. Asegúrate de subir el CSV y el PDF.")
        st.stop()

    empleados = [e.strip() for e in empleados_input.split(',') if e.strip()]
    if not empleados:
        st.error("❌ Error: Necesitas ingresar al menos un nombre en el equipo.")
        st.stop()

    with st.spinner("Analizando documentos..."):
        plataforma, codificacion = detectar_plataforma_web(archivo_csv)
        if plataforma == 'DESCONOCIDA':
            st.error("❌ ERROR: No pude identificar si el CSV es de Temu, TikTok o Shein. Verifica las columnas.")
            st.stop()
            
        st.success(f"¡Plataforma detectada!: **{plataforma}**")

        # --- LEYENDO BASE ---
        diccionario_nombres = {}
        if archivo_base:
            try:
                df_base = pd.read_excel(archivo_base, sheet_name='BASE')
                df_base.columns = df_base.columns.str.strip().str.upper() 
                if 'SKU' in df_base.columns and 'NOMBRE PLATAFORMA' in df_base.columns:
                    for idx, fila in df_base.iterrows():
                        sku = str(fila['SKU']).strip()
                        nombre = str(fila['NOMBRE PLATAFORMA']).strip()
                        if pd.notna(sku) and sku != 'nan':
                            diccionario_nombres[sku] = nombre
            except Exception as e:
                st.warning(f"⚠️ Hubo un detalle al leer tu hoja BASE: {e}")

        # --- LEYENDO PDF ---
        paginas_por_po = {}
        reader = PyPDF2.PdfReader(archivo_pdf)
        
        if plataforma == 'TEMU':
            patron_pdf = r'PO-\d{3}-\d+'
        elif plataforma == 'SHEIN':
            patron_pdf = r'(JMX\d+|GSH\w+)' 
        else:
            patron_pdf = r'(JMX\d+)'
            
        po_actual = None 
        
        for num_pagina, pagina in enumerate(reader.pages):
            texto = pagina.extract_text()
            matches = re.findall(patron_pdf, texto) if texto else []
            
            if matches:
                po_encontrado = str(matches[0]).strip()
                po_actual = po_encontrado 
                
                if po_actual not in paginas_por_po:
                    paginas_por_po[po_actual] = []
                    if plataforma == 'TEMU' and num_pagina > 0:
                        paginas_por_po[po_actual].append(reader.pages[num_pagina - 1])
                
                if pagina not in paginas_por_po[po_actual]:
                    paginas_por_po[po_actual].append(pagina)
            else:
                if plataforma in ['TIKTOK', 'SHEIN'] and po_actual:
                    if pagina not in paginas_por_po[po_actual]:
                        paginas_por_po[po_actual].append(pagina)

        lista_pos_unicos = list(paginas_por_po.keys())
        st.info(f"📄 Se encontraron {len(lista_pos_unicos)} pedidos en el PDF.")

        # --- CRUZANDO CON CSV ---
        texto_csv = archivo_csv.getvalue().decode(codificacion)
        lineas = texto_csv.splitlines()
        skip_lineas = 0
        for i, linea in enumerate(lineas):
            if (plataforma == 'TEMU' and 'ID del pedido' in linea) or \
               (plataforma == 'TIKTOK' and 'Order ID' in linea) or \
               (plataforma == 'SHEIN' and 'Número de pedido' in linea):
                skip_lineas = i
                break
                
        archivo_csv.seek(0) 
        df = pd.read_csv(archivo_csv, skiprows=skip_lineas, encoding=codificacion)
        df.columns = df.columns.str.strip()

        if plataforma == 'TEMU':
            columnas_utiles = ['ID del pedido', 'sku de contribución', 'nombre del producto', 'variación', 'cantidad a enviar']
            df_filtrado = df[columnas_utiles].copy()
            df_filtrado.rename(columns={
                'ID del pedido': 'PEDIDO', 'sku de contribución': 'SKU',
                'nombre del producto': 'NOMBRE_ORIGINAL', 'variación': 'VARIACION',
                'cantidad a enviar': 'CANTIDAD'
            }, inplace=True)
            df_filtrado['PEDIDO_DISPLAY'] = df_filtrado['PEDIDO']
            
        elif plataforma == 'TIKTOK':
            columnas_utiles = ['Order ID', 'Seller SKU', 'Product Name', 'Variation', 'Quantity', 'Tracking ID']
            columnas_existentes = [col for col in columnas_utiles if col in df.columns]
            df_filtrado = df[columnas_existentes].copy()
            if 'Order ID' in df_filtrado.columns and 'Product Name' in df_filtrado.columns and 'Variation' in df_filtrado.columns:
                df_filtrado = df_filtrado.drop_duplicates(subset=['Order ID', 'Product Name', 'Variation'])
            df_filtrado.rename(columns={
                'Tracking ID': 'PEDIDO', 'Seller SKU': 'SKU',
                'Product Name': 'NOMBRE_ORIGINAL', 'Variation': 'VARIACION',
                'Quantity': 'CANTIDAD'
            }, inplace=True)
            df_filtrado['PEDIDO_DISPLAY'] = df_filtrado['PEDIDO']
            
        elif plataforma == 'SHEIN':
            columnas_utiles = ['Número de pedido', 'SKU del vendedor', 'Nombre del producto', 'Especificación', 'Número de guía']
            columnas_existentes = [col for col in columnas_utiles if col in df.columns]
            df_filtrado = df[columnas_existentes].copy()
            
            # Shein no trae cantidad agrupada, cada fila es 1 pieza
            df_filtrado['CANTIDAD'] = 1
            
            # Logica Híbrida: Usar Guía JMX si existe, si no, usar Pedido GSH
            def get_shein_po(row):
                guia = str(row.get('Número de guía', '')).strip()
                if guia.startswith('JMX'):
                    return guia
                return str(row.get('Número de pedido', '')).strip()
                
            df_filtrado['PEDIDO_MATCH'] = df_filtrado.apply(get_shein_po, axis=1)
            df_filtrado['PEDIDO_DISPLAY'] = df_filtrado['Número de pedido'].astype(str).str.strip()
            
            df_filtrado.rename(columns={
                'PEDIDO_MATCH': 'PEDIDO', 'SKU del vendedor': 'SKU',
                'Nombre del producto': 'NOMBRE_ORIGINAL', 'Especificación': 'VARIACION'
            }, inplace=True)

        df_filtrado = df_filtrado.dropna(subset=['PEDIDO'])
        df_filtrado['PEDIDO'] = df_filtrado['PEDIDO'].astype(str).apply(lambda x: x.replace('.0', '') if x.endswith('.0') else x).str.strip()
        df_filtrado['CANTIDAD'] = pd.to_numeric(df_filtrado['CANTIDAD'], errors='coerce').fillna(0)
        df_filtrado = df_filtrado[df_filtrado['CANTIDAD'] > 0]

        df_filtrado['Nombre Correcto'] = df_filtrado.apply(
            lambda fila: limpiar_nombre(
                diccionario_nombres.get(
                    str(fila['SKU']).strip(),
                    f"{fila.get('NOMBRE_ORIGINAL', '')} - Var: {fila.get('VARIACION', 'N/A')}" 
                )
            ), axis=1
        )

        filas_ordenadas = []
        for po in lista_pos_unicos:
            datos_po = df_filtrado[df_filtrado['PEDIDO'] == po]
            if not datos_po.empty:
                filas_ordenadas.append(datos_po)

        df_ordenado = pd.concat(filas_ordenadas) if filas_ordenadas else pd.DataFrame()

        if df_ordenado.empty:
            st.error("❌ ERROR: Ningún pedido del PDF coincidió con el CSV.")
            st.stop()

        df_ordenado['PEDIDO'] = pd.Categorical(df_ordenado['PEDIDO'], categories=lista_pos_unicos, ordered=True)
        df_ordenado = df_ordenado.sort_values('PEDIDO')

        # --- REPARTICIÓN Y CREACIÓN DE ARCHIVOS EN MEMORIA ---
        num_empleados = len(empleados)
        pos_base = len(lista_pos_unicos) // num_empleados
        sobrantes = len(lista_pos_unicos) % num_empleados
        cantidades_por_empleado = [pos_base + (1 if i < sobrantes else 0) for i in range(num_empleados)]

        colores_division = ['#FFD966', '#A9D08E', '#9BC2E6', '#F4B084', '#B4A7D6', '#93CDDD']

        zip_buffer = io.BytesIO()
        with zipfile.ZipFile(zip_buffer, "a", zipfile.ZIP_DEFLATED, False) as zip_file:
            
            excel_buffer = io.BytesIO()
            with pd.ExcelWriter(excel_buffer, engine='xlsxwriter') as writer:
                indice_inicio = 0
                for i, emp in enumerate(empleados):
                    indice_fin = indice_inicio + cantidades_por_empleado[i]
                    pos_del_empleado = lista_pos_unicos[indice_inicio:indice_fin]
                    indice_inicio = indice_fin
                    
                    df_emp = df_ordenado[df_ordenado['PEDIDO'].isin(pos_del_empleado)].copy()
                    
                    if not df_emp.empty:
                        color_actual = colores_division[i % len(colores_division)]
                        
                        # 1. Crear Pestañas Principales del Excel
                        worksheet = writer.book.add_worksheet(emp)
                        formato_titulo = writer.book.add_format({'bold': True, 'font_size': 14, 'bg_color': color_actual, 'border': 1})
                        worksheet.write(0, 0, f"LISTA DE RECOLECCIÓN PARA: {emp.upper()}", formato_titulo)
                        worksheet.write(1, 0, f"Total de guías asignadas: {len(pos_del_empleado)}")
                        
                        # --- TABLA SUPERIOR (Agrupada) ---
                        picking_list = df_emp.groupby(['SKU', 'Nombre Correcto'], sort=False)['CANTIDAD'].sum().reset_index()
                        picking_list.rename(columns={'Nombre Correcto': 'Descripción (Según BASE)', 'CANTIDAD': 'Total a Recolectar'}, inplace=True)
                        picking_list = picking_list.sort_values(by='Descripción (Según BASE)').reset_index(drop=True)

                        inicio_t1 = 3
                        fin_t1 = inicio_t1 + len(picking_list)
                        picking_list.to_excel(writer, sheet_name=emp, index=False, header=False, startrow=inicio_t1 + 1, startcol=0)
                        worksheet.add_table(inicio_t1, 0, fin_t1, len(picking_list.columns) - 1, {
                            'columns': [{'header': col} for col in picking_list.columns], 'style': 'Table Style Medium 9'
                        })
                        worksheet.set_column('A:A', 20)
                        worksheet.set_column('B:B', 65)

                        # --- TABLA INFERIOR (Orden Exacto mostrando el Número Oficial GSH/JMX/PO) ---
                        fila_orden = fin_t1 + 3
                        worksheet.write(fila_orden, 0, f"ORDEN EXACTO DE GUÍAS DE {emp.upper()}:", formato_titulo)
                        
                        df_orden = df_emp[['PEDIDO_DISPLAY', 'SKU', 'Nombre Correcto', 'CANTIDAD']].copy()
                        df_orden.rename(columns={'PEDIDO_DISPLAY': 'PEDIDO', 'CANTIDAD': 'Cant.'}, inplace=True)
                        df_orden.to_excel(writer, sheet_name=emp, index=False, startrow=fila_orden + 2, startcol=0)
                        
                        # ---------------------------------------------------------
                        # 2. CREAR LA HOJA DE TICKETS (Formato para Térmica)
                        # ---------------------------------------------------------
                        hoja_ticket = writer.book.add_worksheet(f"{emp}_Ticket")
                        
                        fmt_header = writer.book.add_format({'bold': True, 'align': 'center', 'valign': 'vcenter', 'bg_color': color_actual, 'border': 1})
                        fmt_titulo_ticket = writer.book.add_format({'bold': True, 'font_size': 14, 'align': 'center', 'valign': 'vcenter', 'bg_color': color_actual, 'border': 1})
                        fmt_td_centro = writer.book.add_format({'border': 1, 'align': 'center', 'valign': 'vcenter', 'text_wrap': True})
                        fmt_td_izq = writer.book.add_format({'border': 1, 'align': 'left', 'valign': 'vcenter', 'text_wrap': True})
                        fmt_total = writer.book.add_format({'bold': True, 'border': 1, 'align': 'center', 'valign': 'vcenter', 'bg_color': '#D9D9D9'})
                        fmt_wrap = writer.book.add_format({'bold': True, 'align': 'center', 'valign': 'vcenter', 'text_wrap': True, 'bg_color': color_actual, 'border': 1})
                        
                        num_division = i + 1
                        hoja_ticket.write('A1', f'DIVISION {num_division}', fmt_header)
                        hoja_ticket.write('D1', plataforma.upper(), fmt_header) 
                        hoja_ticket.merge_range('A2:D2', emp.upper(), fmt_titulo_ticket)
                        
                        encabezados = ['NO', 'SKU', 'NOMBRE COMUN', 'CANTI\nDAD']
                        for col, encabezado in enumerate(encabezados):
                            if encabezado == 'CANTI\nDAD':
                                hoja_ticket.write(3, col, encabezado, fmt_wrap)
                            else:
                                hoja_ticket.write(3, col, encabezado, fmt_header)
                            
                        total_piezas = 0
                        fila = 4
                        
                        for idx, item in picking_list.iterrows():
                            cant = int(item['Total a Recolectar'])
                            total_piezas += cant
                            
                            hoja_ticket.write(fila, 0, idx + 1, fmt_td_centro) 
                            hoja_ticket.write(fila, 1, item['SKU'], fmt_td_centro)            
                            hoja_ticket.write(fila, 2, item['Descripción (Según BASE)'], fmt_td_izq)  
                            hoja_ticket.write(fila, 3, cant, fmt_td_centro)     
                            fila += 1
                            
                        hoja_ticket.write(fila, 0, len(picking_list) + 1, fmt_td_centro)
                        hoja_ticket.merge_range(fila, 1, fila, 2, 'Total general', fmt_total)
                        hoja_ticket.write(fila, 3, total_piezas, fmt_total)
                        
                        hoja_ticket.set_column('A:A', 4)
                        hoja_ticket.set_column('B:B', 16)
                        hoja_ticket.set_column('C:C', 38)
                        hoja_ticket.set_column('D:D', 6)
                        hoja_ticket.set_row(3, 30) 
                        hoja_ticket.set_row(1, 25) 
                        
                        hoja_ticket.fit_to_pages(1, 0) 
                        hoja_ticket.set_margins(left=0.1, right=0.1, top=0.1, bottom=0.1) 
                        
                        # ---------------------------------------------------------
                        # 3. CREAR PDFs EN MEMORIA
                        # ---------------------------------------------------------
                        pdf_writer = PyPDF2.PdfWriter()
                        for po in pos_del_empleado:
                            if po in paginas_por_po:
                                for pagina in paginas_por_po[po]:
                                    pdf_writer.add_page(pagina)

                        pdf_buffer = io.BytesIO()
                        pdf_writer.write(pdf_buffer)
                        zip_file.writestr(f"Guias_{emp}.pdf", pdf_buffer.getvalue())

            zip_file.writestr("Reparticion_Automatizada.xlsx", excel_buffer.getvalue())

        st.session_state['descarga_lista'] = zip_buffer.getvalue()

if 'descarga_lista' in st.session_state:
    st.balloons()
    st.success("✨ ¡Todo listo! Se ha generado un archivo ZIP con el Excel de repartición y los PDFs individuales.")
    
    st.download_button(
        label="📦 Descargar Todos los Documentos (ZIP)",
        data=st.session_state['descarga_lista'],
        file_name="Documentos_Vmingo.zip",
        mime="application/zip",
        type="primary"
    )
