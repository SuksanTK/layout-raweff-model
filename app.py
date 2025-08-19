import streamlit as st
import pandas as pd
import io

# ========== ตั้งค่าหน้าเว็บ ========== #
st.set_page_config(page_title="Data Processing App", layout="wide")
st.title("⚙️ Data Processing Automation")
st.markdown("อัปโหลดไฟล์ > เลือกแท็บ > กดปุ่มประมวลผล > ดาวน์โหลดผลลัพธ์")

# ========== ฟังก์ชันการทำงานของโค้ดชุดที่ 1 (เหมือนเดิม) ========== #
def process_layout_joiner(layout_file, stylelist_file):
    try:
        layout_master = pd.read_csv(layout_file, encoding='utf-8-sig')
        stylelistcode = pd.read_csv(stylelist_file, encoding='utf-8-sig')
        merged_df = pd.merge(
            layout_master, 
            stylelistcode, 
            how='inner', 
            left_on='LINELAYOUT', 
            right_on='LINELAYOUT'
        )
        return merged_df
    except Exception as e:
        st.error(f"เกิดข้อผิดพลาดใน Layout Joiner: {e}")
        return None

# ========== ฟังก์ชันการทำงานของโค้ดชุดที่ 2 (เพิ่มส่วนตรวจสอบ) ========== #
def process_rawdata_model(rawdata_file, stylelist_file):
    try:
        rawdata_df = pd.read_csv(rawdata_file, encoding='utf-8-sig')
        stylelistcode_df = pd.read_csv(stylelist_file, encoding='utf-8-sig')

        rawdata_df.columns = rawdata_df.columns.str.strip().str.lower()
        stylelistcode_df.columns = stylelistcode_df.columns.str.strip().str.lower()

        # --- DEBUG 1: ตรวจสอบคอลัมน์ "ก่อน" การ Merge ---
        st.info("ขั้นตอนที่ 1: ตรวจสอบคอลัมน์ 'ก่อน' การ Merge")
        st.write("**คอลัมน์ใน `RawdataALL.csv`:**")
        st.write(rawdata_df.columns.tolist())
        st.write("**คอลัมน์ใน `stylelistcode.csv`:**")
        st.write(stylelistcode_df.columns.tolist())
        # -----------------------------------------------

        merged_df = pd.merge(rawdata_df, stylelistcode_df, on='group', how='inner')
        
        # --- DEBUG 2: ตรวจสอบคอลัมน์ "หลัง" การ Merge ---
        st.info("ขั้นตอนที่ 2: ตรวจสอบคอลัมน์ 'หลัง' การ Merge")
        st.write("**คอลัมน์ทั้งหมดหลังจากการ Merge คือ:**")
        st.write(merged_df.columns.tolist())
        # --------------------------------------------

        # ถ้าเราเจอคอลัมน์ชื่อ 'line_x' ให้เปลี่ยนชื่อกลับมาเป็น 'line'
        if 'line_x' in merged_df.columns:
            st.warning("ตรวจพบคอลัมน์ 'line_x' (อาจเกิดจากชื่อซ้ำกัน) กำลังเปลี่ยนชื่อกลับเป็น 'line'")
            merged_df = merged_df.rename(columns={'line_x': 'line'})

        required_columns = ['line', 'linkeff', 'linkop', 'id', 'shift', 'style', 'group', 'jobtitle', 'eff']
        for col in required_columns:
            if col not in merged_df.columns:
                st.error(f"หยุดการทำงาน: ไม่พบคอลัมน์ที่จำเป็น '{col}' กรุณาตรวจสอบผลการตรวจสอบด้านบน")
                return None

        merged_df['eff'] = pd.to_numeric(merged_df['eff'], errors='coerce').fillna(0)
        merged_df['eff_adjusted'] = merged_df['eff'] * 1.05
        merged_df['rank'] = merged_df.groupby(['id', 'group', 'jobtitle'])['eff_adjusted'] \
                                     .rank(method='first', ascending=False)
        top3_df = merged_df[(merged_df['rank'] <= 3) & (merged_df['eff'] >= 35)]
        
        if top3_df.empty:
            st.warning("ไม่พบข้อมูลที่ตรงตามเงื่อนไขการกรอง (rank <= 3 และ eff >= 35) จึงไม่มีผลลัพธ์")
            return pd.DataFrame()

        agg_df = top3_df.groupby(['linkeff', 'linkop', 'id', 'line', 'shift', 'style', 'group', 'jobtitle'], as_index=False)['eff'].mean()
        agg_df = agg_df.rename(columns={'eff': 'AvgEff'})
        
        return agg_df
    except Exception as e:
        st.error(f"เกิดข้อผิดพลาดใน Raw Data Processor: {e}")
        return None

# ========== ส่วนของ UI (เหมือนเดิม) ========== #
with st.sidebar:
    st.header("📂 อัปโหลดไฟล์ CSV")
    uploaded_layout_master = st.file_uploader("1. อัปโหลด layout_master.csv", type=["csv"])
    uploaded_stylelistcode = st.file_uploader("2. อัปโหลด stylelistcode.csv", type=["csv"])
    uploaded_rawdata_all = st.file_uploader("3. อัปโหลด RawdataALL.csv", type=["csv"])
tab1, tab2 = st.tabs(["Process 1: Layout Joiner", "Process 2: Raw Data Model"])
with tab1:
    st.header("🔗 รวมไฟล์ Layout Master และ Style List")
    st.markdown("ใช้ไฟล์ `layout_master.csv` และ `stylelistcode.csv`")
    if st.button("🚀 เริ่มประมวลผล Layout Joiner", key="btn1"):
        if uploaded_layout_master and uploaded_stylelistcode:
            with st.spinner('กำลังรวมไฟล์...'):
                st.session_state.df_result1 = process_layout_joiner(uploaded_layout_master, uploaded_stylelistcode)
            if st.session_state.df_result1 is not None:
                st.success("✅ รวมไฟล์เรียบร้อยแล้ว!")
        else:
            st.warning("⚠️ กรุณาอัปโหลดไฟล์ `layout_master.csv` และ `stylelistcode.csv` ก่อน")
    if 'df_result1' in st.session_state and st.session_state.df_result1 is not None:
        st.subheader("📊 ผลลัพธ์:")
        st.dataframe(st.session_state.df_result1)
        csv_data = st.session_state.df_result1.to_csv(index=False).encode('utf-8')
        st.download_button(label="📥 ดาวน์โหลดผลลัพธ์ (Layout_week18_22.csv)",data=csv_data,file_name='Layout_week18_22.csv',mime='text/csv')
with tab2:
    st.header("📈 ประมวลผล Raw Data เพื่อสร้างโมเดล")
    st.markdown("ใช้ไฟล์ `RawdataALL.csv` และ `stylelistcode.csv`")
    if st.button("🚀 เริ่มประมวลผล Raw Data Model", key="btn2"):
        if uploaded_rawdata_all and uploaded_stylelistcode:
            with st.spinner('กำลังประมวลผลข้อมูล...'):
                st.session_state.df_result2 = process_rawdata_model(uploaded_rawdata_all, uploaded_stylelistcode)
            if 'df_result2' in st.session_state and st.session_state.df_result2 is not None:
                st.success("✅ ประมวลผลข้อมูลเรียบร้อยแล้ว!")
        else:
            st.warning("⚠️ กรุณาอัปโหลดไฟล์ `RawdataALL.csv` และ `stylelistcode.csv` ก่อน")
    if 'df_result2' in st.session_state and st.session_state.df_result2 is not None:
        st.subheader("📊 ผลลัพธ์:")
        st.dataframe(st.session_state.df_result2)
        csv_data = st.session_state.df_result2.to_csv(index=False).encode('utf-8')
        st.download_button(label="📥 ดาวน์โหลดผลลัพธ์ (RAWDATA_MODEL_ALL1.csv)",data=csv_data,file_name='RAWDATA_MODEL_ALL1.csv',mime='text/csv')
