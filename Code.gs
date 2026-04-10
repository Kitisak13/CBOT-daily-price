/**
 * Daily CME Futures Price Scraper (Google Apps Script)
 * ====================================================
 * Note: CME Group uses strict anti-scraping protections. 
 * Requests from Google Data Centers (which Apps Script uses) 
 * might be blocked with a 403 Forbidden error.
 */

const PRODUCTS = [
  { name: "Corn", product_id: 300, slug: "corn" },
  { name: "Wheat", product_id: 323, slug: "wheat" },
  { name: "Soybean", product_id: 320, slug: "soybean" },
  { name: "Soybean Meal", product_id: 310, slug: "soybean-meal" },
  { name: "Soybean Oil", product_id: 312, slug: "soybean-oil" },
  { name: "Rough Rice", product_id: 336, slug: "rough-rice" },
  { name: "Sugar No.11", product_id: 470, slug: "sugar-no11" },
  { name: "Dutch TTF Natural Gas", product_id: 8337, slug: "dutch-ttf-natural-gas-usd-mmbtu-icis-heren-front-month" },
  { name: "Light Sweet Crude (WTI)", product_id: 425, slug: "light-sweet-crude" },
  { name: "Brent Crude Oil", product_id: 421, slug: "brent-crude-oil" }
];

// ชิ้อ Sheet ที่ต้องการให้บันทึกข้อมูล
const SHEET_NAME = 'Sheet1'; 

function runScraper() {
  const sheet = SpreadsheetApp.getActiveSpreadsheet().getSheetByName(SHEET_NAME);
  
  // สร้าง Header หากแผ่นงานยังว่างอยู่
  if (sheet.getLastRow() === 0) {
    sheet.appendRow([
      "Date", "Product", "Month", "Open", "High", 
      "Low", "Last", "Change", "Settle", "Est. Volume", "Prior Day OI"
    ]);
  }

  // วันที่สำหรับบันทึก
  const today = Utilities.formatDate(new Date(), 'Asia/Bangkok', 'yyyy-MM-dd');
  
  // จำลอง HTTP Header ให้เหมือนเบราว์เซอร์ปกติ
  const headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36 Edg/125.0.0.0',
    'Accept': 'application/json, text/plain, */*'
  };

  const options = {
    'method' : 'get',
    'headers' : headers,
    'muteHttpExceptions': true
  };

  PRODUCTS.forEach(product => {
    try {
      // 1. ดึงข้อมูลวันที่เทรดที่อัปเดตล่าสุด
      const dateUrl = `https://www.cmegroup.com/CmeWS/mvc/Settlements/Futures/TradeDate/${product.product_id}?isProtected`;
      const dateResponse = UrlFetchApp.fetch(dateUrl, options);
      
      if (dateResponse.getResponseCode() !== 200) {
         Logger.log(`Failed to fetch date for ${product.name}: HTTP ${dateResponse.getResponseCode()} (อาจถูกบล็อคโดยระบบป้องกัน Bot)`);
         return;
      }
      
      const tradeDates = JSON.parse(dateResponse.getContentText());
      const latestDate = tradeDates[0][0]; // ตัวอย่าง "04/09/2026"
      
      // 2. ดึงข้อมูลตัวเลขและราคาเทรดอิงจากวันที่ล่าสุด
      const settlementsUrl = `https://www.cmegroup.com/CmeWS/mvc/Settlements/Futures/Settlements/${product.product_id}/FUT?strategy=DEFAULT&tradeDate=${latestDate}&pageSize=500&isProtected`;
      const setResponse = UrlFetchApp.fetch(settlementsUrl, options);

      if (setResponse.getResponseCode() !== 200) {
         Logger.log(`Failed to fetch data for ${product.name}: HTTP ${setResponse.getResponseCode()}`);
         return;
      }

      const data = JSON.parse(setResponse.getContentText());
      const settlements = data.settlements || [];
      
      // 3. เอาเฉพาะลำดับแรกและเพิ่มแถวเข้าสู่ Sheet
      if (settlements.length > 0) {
        const first = settlements[0];
        const rowData = [
          today,
          product.slug,
          first.month || "-",
          first.open || "-",
          first.high || "-",
          first.low || "-",
          first.last || "-",
          first.change || "-",
          first.settle || "-",
          first.volume || "-",
          first.openInterest || "-"
        ];
        sheet.appendRow(rowData);
        Logger.log(`[OK] Saved -> ${product.name}`);
      }
      
    } catch (e) {
       Logger.log(`Error on ${product.name}: ${e.message}`);
    }
    
    // ดีเลย์เวลาเล็กน้อยแบบสุ่มเพื่อชะลอการส่ง Request ไม่ให้รัวจนเกินไป
    Utilities.sleep(Math.random() * 2000 + 1000); 
  });
}

/**
 * สร้างตั้งเวลาการทำการรายวันเวลา 12:30 แบบเดียวกันกับ Python
 * โดยการเข้าไปที่หน้าต่าง Triggers ของ Apps Script > ทำการเลือกฟังก์ชัน Create Trigger (รูปนาฬิกา)
 * ละเลือก runScraper -> Time-driven -> Day timer -> 12pm to 1pm.
 */
