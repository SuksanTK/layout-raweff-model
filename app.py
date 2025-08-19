import streamlit as st
import pandas as pd
import io

# ... (ส่วนโค้ดอื่นๆ ที่เหมือนเดิม) ...
st.set_page_config(page_title="Data Processing App", layout="wide")
st.title("⚙️ Data Processing Automation")
st.markdown("อัปโหลดไฟล์ > เลือกแท็บ > กดปุ่มประมวลผล > ดาวน์โหลดผลลัพธ์")

def process_layout_joiner(layout_file, stylelist_file):
    try:
        layout_master = pd.read_csv(layout_file, encoding='tis-620')
        stylelistcode = pd.read_csv(stylelist_file, encoding='tis-620')
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

# ========== ฟังก์ชันที่ทำการแก้ไข ========== #
def process_rawdata_model(rawdata_file, stylelist_file):
    try:
        rawdata_df = pd.read_csv(rawdata_file, encoding='tis-620')
        stylelistcode_df = pd.read_csv(stylelist_file, encoding='tis-620')

        rawdata_df.columns = rawdata_df.columns.str.strip().str.lower()
        stylelistcode_df.columns = stylelistcode_df.columns.str.strip().str.lower()
        
        # --- DEBUG 1: ตรวจสอบคอลัมน์หลังอ่านไฟล์ ---
        st.info("ขั้นตอนที่ 1: ตรวจสอบคอลัมน์จาก RawdataALL.csv")
        st.write("คอลัมน์ทั้งหมด (หลังแปลงเป็นตัวพิมพ์เล็กและตัดช่องว่าง):")
        st.write(rawdata_df.columns.tolist())
        # -----------------------------------------

        merged_df = pd.merge(rawdata_df, stylelistcode_df, on='group', how='inner')
        
        # --- DEBUG 2: ตรวจสอบคอลัมน์และข้อมูลหลัง Merge ---
        st.info("ขั้นตอนที่ 2: ตรวจสอบข้อมูลหลังการ Merge")
        st.write("คอลัมน์ทั้งหมดที่มีอยู่หลังการ Merge:")
        st.write(merged_df.columns.tolist())
        st.write(f"จำนวนแถวทั้งหมดหลัง Merge: {len(merged_df)}")
        st.dataframe(merged_df.head())
        # ---------------------------------------------

        required_columns = ['line', 'linkeff', 'linkop', 'id', 'shift', 'style', 'group', 'jobtitle', 'eff']
        for col in required_columns:
            if col not in merged_df.columns:
                # ถ้าคอลัมน์ที่จำเป็นยังไม่มีอยู่ ให้หยุดทำงานและแจ้งเตือน
                st.error(f"หยุดการทำงาน: ไม่พบคอลัมน์ '{col}' ที่จำเป็นหลังจากการ Merge")
                return None

        merged_df['eff'] = pd.to_numeric(merged_df['eff'], errors='coerce').fillna(0)
        merged_df['eff_adjusted'] = merged_df['eff'] * 1.05
        merged_df['rank'] = merged_df.groupby(['id', 'group', 'jobtitle'])['eff_adjusted'] \
                                     .rank(method='first', ascending=False)
        top3_df = merged_df[(merged_df['rank'] <= 3) & (merged_df['eff'] >= 35)]
        
        # --- DEBUG 3: ตรวจสอบข้อมูลก่อนทำ Groupby ---
        st.info("ขั้นตอนที่ 3: ตรวจสอบข้อมูลที่จะนำไป Groupby")
        st.write(f"จำนวนแถวหลังการกรอง (rank <= 3 และ eff >= 35): {len(top3_df)}")
        st.dataframe(top3_df.head())
        # -----------------------------------------
        
        # ถ้าไม่มีข้อมูลเหลือหลังจากการกรอง ให้แจ้งเตือนและหยุด
        if top3_df.empty:
            st.warning("ไม่พบข้อมูลที่ตรงตามเงื่อนไขการกรอง (rank <= 3 และ eff >= 35) จึงไม่มีผลลัพธ์")
            return pd.DataFrame() # คืนค่าเป็น DataFrame ว่าง

        agg_df = top3_df.groupby(['linkeff', 'linkop', 'id', 'line', 'shift', 'style', 'group', 'jobtitle'], as_index=False)['eff'].mean()
        agg_df = agg_df.rename(columns={'eff': 'AvgEff'})
        
        return agg_df
    except KeyError as e:
        st.error(f"เกิด KeyError: ไม่พบคอลัมน์ {e} กรุณาตรวจสอบว่าชื่อคอลัมน์ในไฟล์ CSV ถูกต้องหรือไม่")
        return None
    except Exception as e:
        st.error(f"เกิดข้อผิดพลาดที่ไม่คาดคิด: {e}")
        return None

# ... (ส่วน UI ที่เหมือนเดิม) ...
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
            if st.session_state.df_result2 is not None:
                st.success("✅ ประมวลผลข้อมูลเรียบร้อยแล้ว!")
        else:
            st.warning("⚠️ กรุณาอัปโหลดไฟล์ `RawdataALL.csv` และ `stylelistcode.csv` ก่อน")
    if 'df_result2' in st.session_state and st.session_state.df_result2 is not None:
        st.subheader("📊 ผลลัพธ์:")
        st.dataframe(st.session_state.df_result2)
        csv_data = st.session_state.df_result2.to_csv(index=False).encode('utf-8')
        st.download_button(label="📥 ดาวน์โหลดผลลัพธ์ (RAWDATA_MODEL_ALL1.csv)",data=csv_data,file_name='RAWDATA_MODEL_ALL1.csv',mime='text/csv')
