function mainProcessing() {
  // === ตั้งค่าตัวแปรหลัก ===
  var scriptProperties = PropertiesService.getScriptProperties();
  var scraperApiKey = scriptProperties.getProperty("scraperApiKey");
  var email = "kitisak.junsong@gmail.com";
  var sheetId = "1C3OpWjT0UgdIj20mfxmT13mvevWbjkMrn3addthBfwM";
  var sheetName = "Sheet1";

  if (!scraperApiKey) {
    Logger.log(
      "ข้อผิดพลาด: ไม่พบ API Key กรุณาเข้าไปตั้งค่าตัวแปร 'scraperApiKey' ใน Project Settings > Script Properties",
    );
    return;
  }

  // 1. เช็ควันที่ล่าสุดด้วยการยิง API ตรงไปที่ฐานข้อมูลของ CME (Product 300 = Corn) แทนการอ่านหน้าเว็บ
  // วิธีนี้เสถียรกว่า 100% เพราะได้เป็นข้อมูลดิบ (JSON) ไม่ต้องกังวลเรื่องเว็บโหลดช้าหรือ Element หาย
  var tradeApiUrl =
    "https://www.cmegroup.com/CmeWS/mvc/Settlements/Futures/TradeDate/300?isProtected";
  // ใช้ render=false เพราะเป็นแค่ API ไม่ต้องเปลืองแรงประมวลผล Javascript ช่วยให้ดึงข้อมูลไวขึ้นแถมประหยัดเครดิต
  var checkDateUrl =
    "http://api.scraperapi.com/?api_key=" +
    scraperApiKey +
    "&url=" +
    encodeURIComponent(tradeApiUrl) +
    "&render=false";

  Logger.log("กำลังเช็ควันที่ล่าสุดจากฐานข้อมูล (API) ของ CME...");
  var response = UrlFetchApp.fetch(checkDateUrl, {
    method: "get",
    muteHttpExceptions: true,
  });

  if (response.getResponseCode() !== 200) {
    Logger.log(
      "ไม่สามารถเชื่อมต่อ CME API ได้: HTTP " + response.getResponseCode(),
    );
    return;
  }

  var textData = extractJsonFromHtml(response.getContentText());
  var latestDate = null;

  try {
    var datesObj = JSON.parse(textData);
    if (datesObj && datesObj.length > 0) {
      // โครงสร้าง API คืนค่าเป็น ID หรือวันที่ เช่น "04/13/2026" หรือ ["04/13/2026", "Monday..."]
      latestDate = datesObj[0][0];
    }
  } catch (e) {
    Logger.log(
      "เกิดข้อผิดพลาดในการอ่านข้อมูล JSON ของ TradeDate: " + e.message,
    );
    return;
  }

  if (latestDate) {
    var lastSavedDate = scriptProperties.getProperty("cme_latest_date");

    Logger.log(
      "วันที่ใน API ล่าสุด: " +
        latestDate +
        " | วันที่บันทึกไว้: " +
        lastSavedDate,
    );

    if (latestDate !== lastSavedDate) {
      Logger.log(
        "🎉 ตรวจพบวันใหม่! กำลังดึงข้อมูลราคาทั้ง 10 โปรดักซ์ เพื่อบันทึกลง Google Sheet...",
      );

      // 2. เรียกใช้งานระบบดึงราคา 10 ผลิตภัณฑ์รวดเดียว โดยส่ง latestDate ไปบันทึกด้วย
      var scrapedCount = scrapeAllProductsToSheet(
        scraperApiKey,
        sheetId,
        sheetName,
        latestDate,
      );

      // 3. ส่งอีเมลสรุป
      var subject = "🔔 อัพเดทราคาตลาด CME - " + latestDate;
      var body =
        "ระบบได้ทำการอัพเดทราคาล่าสุดของตลาด CME Group\n" +
        "ประจำวันที่: " +
        latestDate +
        "\n\n" +
        "ได้บันทึกข้อมูลจำนวน " +
        scrapedCount +
        " รายการลงใน Google Sheet เรียบร้อยแล้วครับ\n\n" +
        "ดูข้อมูลได้ที่: https://docs.google.com/spreadsheets/d/" +
        sheetId +
        "/edit\n\n" +
        "** ระบบอัตโนมัติ Google Apps Script **";

      GmailApp.sendEmail(email, subject, body);

      // บันทึกวันที่ใหม่
      scriptProperties.setProperty("cme_latest_date", latestDate);
      Logger.log("เสร็จสิ้นการทำงานทั้งหมด ✅");
    } else {
      Logger.log("ยังไม่มีการอัพเดทข้อมูลใหม่ ข้ามการดึงราคา");
    }
  } else {
    Logger.log("ดึงข้อมูลสำเร็จ แต่รูปแบบข้อมูลวันที่ว่างเปล่า");
  }
}

// ฟังก์ชันช่วยยิงรีเควสแบ่งเป็นชุดๆ ป้องกันปัญหา Concurrency Limit ของ ScraperAPI (ฟรีให้ยิงพร้อมกันแค่ 5 ตัว)
function fetchInBatches(requests, batchSize) {
  var allResponses = [];
  for (var i = 0; i < requests.length; i += batchSize) {
    var chunk = requests.slice(i, i + batchSize);
    var responses = UrlFetchApp.fetchAll(chunk);
    allResponses = allResponses.concat(responses);
    if (i + batchSize < requests.length) {
      Utilities.sleep(1000); // พักสัก 1 วินาที ลดภาระเซิร์ฟเวอร์
    }
  }
  return allResponses;
}

function scrapeAllProductsToSheet(apiKey, sheetId, sheetName, tradeDate) {
  var sheet = SpreadsheetApp.openById(sheetId).getSheetByName(sheetName);
  var products = [
    { name: "Corn", product_id: 300, slug: "corn" },
    { name: "Wheat", product_id: 323, slug: "wheat" },
    { name: "Soybean", product_id: 320, slug: "soybean" },
    { name: "Soybean Meal", product_id: 310, slug: "soybean-meal" },
    { name: "Soybean Oil", product_id: 312, slug: "soybean-oil" },
    { name: "Rough Rice", product_id: 336, slug: "rough-rice" },
    { name: "Sugar No.11", product_id: 470, slug: "sugar-no11" },
    {
      name: "Dutch TTF Natural Gas",
      product_id: 8337,
      slug: "dutch-ttf-natural-gas",
    },
    {
      name: "Light Sweet Crude (WTI)",
      product_id: 425,
      slug: "light-sweet-crude",
    },
    { name: "Brent Crude Oil", product_id: 421, slug: "brent-crude-oil" },
  ];

  // ==============================================================
  // STEP A: ดึง Trade Date (ID ของวันล่าสุด) ของทุกผลิตภัณฑ์ (แบบขนาน ให้ไวขึ้น)
  // ==============================================================
  Logger.log("กำลังดึงรหัสวันที่ของสินค้า...");
  var tradeRequests = products.map(function (p) {
    var url =
      "https://www.cmegroup.com/CmeWS/mvc/Settlements/Futures/TradeDate/" +
      p.product_id +
      "?isProtected";
    // ใช้ render=false เปลี่ยนจาก html ให้เร็วขึ้นมาก
    var api =
      "http://api.scraperapi.com/?api_key=" +
      apiKey +
      "&url=" +
      encodeURIComponent(url) +
      "&render=false";
    return { url: api, muteHttpExceptions: true };
  });

  // แบ่งยิงทีละ 5 อัน (แก้ปัญหาดึงมาไม่ครบ)
  var tradeResponses = fetchInBatches(tradeRequests, 5);

  var settlementRequests = [];
  var validProducts = [];

  for (var i = 0; i < products.length; i++) {
    if (tradeResponses[i].getResponseCode() === 200) {
      var text = extractJsonFromHtml(tradeResponses[i].getContentText());
      try {
        var tradeDatesObj = JSON.parse(text);
        if (tradeDatesObj && tradeDatesObj.length > 0) {
          var latestTradeId = tradeDatesObj[0][0]; // "04/13/2026"

          var settleUrl =
            "https://www.cmegroup.com/CmeWS/mvc/Settlements/Futures/Settlements/" +
            products[i].product_id +
            "/FUT?strategy=DEFAULT&tradeDate=" +
            latestTradeId +
            "&pageSize=500&isProtected";

          // ใช้ render=false เช่นกัน
          var apiSettle =
            "http://api.scraperapi.com/?api_key=" +
            apiKey +
            "&url=" +
            encodeURIComponent(settleUrl) +
            "&render=false";
          settlementRequests.push({ url: apiSettle, muteHttpExceptions: true });
          validProducts.push(products[i]);
        }
      } catch (e) {}
    }
  }

  if (settlementRequests.length === 0) return 0;

  // ==============================================================
  // STEP B: ดึงราคา Settlements ของทุกผลิตภัณฑ์ (แบบขนานพร้อมๆกัน)
  // ==============================================================
  Logger.log(
    "กำลังดึงราคาสินค้าทั้ง " +
      settlementRequests.length +
      " รายการพร้อมๆกัน...",
  );
  var settleResponses = fetchInBatches(settlementRequests, 5);

  var rowsToAppend = [];

  for (var i = 0; i < validProducts.length; i++) {
    var p = validProducts[i];
    if (settleResponses[i].getResponseCode() === 200) {
      var text = extractJsonFromHtml(settleResponses[i].getContentText());
      try {
        var data = JSON.parse(text);
        var settlements = data.settlements || [];
        if (settlements.length > 0) {
          var first = settlements[0];
          // คอลัมน์: Date, Product, Month, Open, High, Low, Last, Change, Settle, Est. Volume, Prior Day OI
          rowsToAppend.push([
            tradeDate,
            p.slug,
            first.month || "-",
            cleanCmeNumber(first.open),
            cleanCmeNumber(first.high),
            cleanCmeNumber(first.low),
            cleanCmeNumber(first.last),
            cleanCmeNumber(first.change),
            cleanCmeNumber(first.settle),
            cleanCmeNumber(first.volume),
            cleanCmeNumber(first.openInterest),
          ]);
        }
      } catch (e) {}
    }
  }

  // ==============================================================
  // STEP C: บันทึกลง Google Sheet
  // ==============================================================
  if (rowsToAppend.length > 0) {
    // ถ้ามีข้อมูลใหม่ ก็เอาไปต่อท้ายแถวเดิม
    var startRow = sheet.getLastRow() + 1;
    sheet
      .getRange(startRow, 1, rowsToAppend.length, rowsToAppend[0].length)
      .setValues(rowsToAppend);
    Logger.log("✔️ บันทึกข้อมูลสำเร็จ " + rowsToAppend.length + " แถว");
  }

  return rowsToAppend.length;
}

// ตัวช่วยสกัด JSON อกมาจาก HTML ในกรณีที่ ScraperAPI หุ้ม JSON ไว้ใน <pre> หรือ <body>
function extractJsonFromHtml(rawContent) {
  var text = rawContent;
  var preMatch = text.match(/<pre[^>]*>([\s\S]*?)<\/pre>/i);
  if (preMatch) text = preMatch[1];
  return text.replace(/<[^>]*>?/gm, "").trim();
}

// ฟังก์ชันสำหรับกำจัดและแปลงรูปแบบตัวเลขของ CME
// CME มักจะใช้ ' แทนจุดทศนิยมสำหรับสินค้าเกษตร (เช่น 446'0 เปลี่ยนเป็น 446.0)
function cleanCmeNumber(val) {
  if (val === undefined || val === null || val === "") return "-";

  var strVal = String(val).trim();

  // แปลง ' เป็น .
  if (strVal.indexOf("'") !== -1) {
    strVal = strVal.replace(/'/g, ".");
  }

  return strVal;
}

// =========================================================
// ตัวช่วยตั้งเวลารันแบบเจาะจงนาที (12:15 PM)
// =========================================================
function setUpDailyTrigger() {
  var timeToRun = "12:15"; // ระบุเวลาที่ต้องการให้รันตรงนี้
  
  // 1. เคลียร์ Trigger ทิ้งก่อนเพื่อป้องกันการสร้างซ้ำซ้อน
  var triggers = ScriptApp.getProjectTriggers();
  for (var i = 0; i < triggers.length; i++) {
    if (triggers[i].getHandlerFunction() === 'mainProcessing') {
      ScriptApp.deleteTrigger(triggers[i]);
    }
  }

  // 2. สร้างออบเจ็กต์ Date สำหรับเวลา 12:15 น. ของวันนี้
  var now = new Date();
  var parts = timeToRun.split(':');
  var hour = parseInt(parts[0], 10);
  var min = parseInt(parts[1], 10);
  
  var triggerTime = new Date();
  triggerTime.setHours(hour, min, 0, 0);

  // ถ้าเวลาปัจจุบันเลยเวลาที่ตั้งไว้แล้ว (เช่น เผลอมากดรันตอน 13:00) ให้ตั้งเวลาเป็น 12:15 ของวันพรุ่งนี้แทน
  if (now.getTime() > triggerTime.getTime()) {
    triggerTime.setDate(triggerTime.getDate() + 1);
  }

  // 3. ปล่อยคำสั่งสร้าง Trigger แบบเจาะจงเวลา (.at)
  ScriptApp.newTrigger('mainProcessing')
    .timeBased()
    .at(triggerTime)
    .create();
    
  Logger.log('ตั้งเวลารัน mainProcessing สำเร็จ: รันในเวลา ' + triggerTime);
}
