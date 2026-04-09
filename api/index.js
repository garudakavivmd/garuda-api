// ================================================================
// GARUDA CONSTRUCTION SERVICE — Vercel API v2
// No Google. No blocking. Free forever.
// ================================================================

const SUPABASE_URL    = process.env.SUPABASE_URL;
const SUPABASE_SECRET = process.env.SUPABASE_SECRET;
const TG_TOKEN        = process.env.TG_TOKEN;
const TG_CHAT         = process.env.TG_CHAT;

async function sb(path, method='GET', body=null) {
  const opts = {
    method,
    headers: {
      'apikey': SUPABASE_SECRET,
      'Authorization': `Bearer ${SUPABASE_SECRET}`,
      'Content-Type': 'application/json',
      'Prefer': method==='POST' ? 'return=representation' : (method==='DELETE' ? 'return=minimal' : '')
    }
  };
  if (body) opts.body = JSON.stringify(body);
  const r = await fetch(`${SUPABASE_URL}/rest/v1/${path}`, opts);
  if (!r.ok) { const e = await r.text(); throw new Error(`DB error: ${e}`); }
  return r.status===204 ? [] : r.json();
}

async function telegram(msg) {
  try {
    await fetch(`https://api.telegram.org/bot${TG_TOKEN}/sendMessage`, {
      method:'POST', headers:{'Content-Type':'application/json'},
      body: JSON.stringify({chat_id:TG_CHAT, text:msg, parse_mode:'Markdown'})
    });
  } catch(e) { console.error('TG:',e); }
}

function fmt(v) { return Number(v||0).toLocaleString('en-IN'); }

function cors(res) {
  res.setHeader('Access-Control-Allow-Origin','*');
  res.setHeader('Access-Control-Allow-Methods','GET,POST,OPTIONS');
  res.setHeader('Access-Control-Allow-Headers','Content-Type');
}
function ok(res,data)  { cors(res); res.status(200).json({status:'ok',...data}); }
function err(res,msg)  { cors(res); res.status(200).json({status:'error',message:msg}); }

export default async function handler(req, res) {
  cors(res);
  if (req.method==='OPTIONS') return res.status(200).end();
  try {
    const action = req.method==='GET' ? req.query.action : (req.body?.action||req.query.action);
    if (req.method==='GET') {
      if (action==='ping')          return ok(res,{message:'Garuda API v2 ✅'});
      if (action==='getStock')      return await getStock(req,res);
      if (action==='getBills')      return await getBills(req,res);
      if (action==='getReturns')    return await getReturns(req,res);
      if (action==='getPending')    return await getPending(req,res);
      if (action==='getExcel')      return await getExcel(req,res);
      if (action==='getBillById')   return await getBillById(req,res);
      return ok(res,{message:'Garuda API v2 ✅'});
    }
    if (req.method==='POST') {
      const data = req.body;
      if (data.action==='addOrder')       return await addOrder(data,res);
      if (data.action==='addReturn')      return await addReturn(data,res);
      if (data.action==='settlePayment')  return await settlePayment(data,res);
      if (data.action==='uploadXL')       return await uploadXL(data,res);
      return err(res,'Unknown action: '+data.action);
    }
    return err(res,'Method not allowed');
  } catch(e) { console.error('Handler:',e); return err(res,e.message); }
}

// ── GET STOCK (BUG FIXED: available = total + in - out) ───────
async function getStock(req,res) {
  const tbl = req.query.sheet==='SUB' ? 'inventory_sub' : 'inventory_main';
  const rows = await sb(`${tbl}?select=*&order=id`);
  const stock = rows.map(r => ({
    name:      r.product,
    total:     Number(r.total_stock)||0,
    in_qty:    Number(r.in_qty)||0,
    out_qty:   Number(r.out_qty)||0,
    // BUG FIX: available = total + in - out (net available after accounting for returns)
    available: (Number(r.total_stock)||0) - (Number(r.out_qty)||0) + (Number(r.in_qty)||0),
    // Only show OUT that hasn't been returned yet
    out:       Math.max(0, (Number(r.out_qty)||0) - (Number(r.in_qty)||0))
  }));
  return ok(res,{stock});
}

// ── GET BILLS ─────────────────────────────────────────────────
async function getBills(req,res) {
  const q = (req.query.q||'').trim().toLowerCase();
  let filter = 'select=*,sale_items(*)&order=id.desc';
  if (q) filter += `&or=(bill_no.ilike.*${q}*,customer.ilike.*${q}*,phone.ilike.*${q}*)`;
  const rows = await sb(`sales?${filter}`);
  const bills = rows.map(r => ({
    billNo:r.bill_no, customer:r.customer, phone:r.phone,
    address:r.address, outDate:r.out_date, inDate:r.in_date,
    days:r.days, months:r.months, advance:r.advance,
    balance:r.balance, total:r.total, status:r.status,
    date: r.created_at ? new Date(r.created_at).toLocaleDateString('en-IN',{day:'2-digit',month:'short',year:'numeric'}) : '',
    sheetType:r.sheet_type,
    items:(r.sale_items||[]).map(i=>({name:i.product,qty:i.qty,price:i.price,type:i.rent_type,amount:i.amount}))
  }));
  return ok(res,{bills,count:bills.length});
}

// ── GET BILL BY ID (for return auto-fill) ────────────────────
async function getBillById(req,res) {
  const id = (req.query.id||'').trim();
  if (!id) return err(res,'Bill ID required');
  const filter = `sales?select=*,sale_items(*)&or=(bill_no.ilike.*${id}*,customer.ilike.*${id}*,phone.ilike.*${id}*)&order=id.desc`;
  const rows = await sb(filter);
  const bills = rows.map(r=>({
    billNo:r.bill_no, customer:r.customer, phone:r.phone,
    address:r.address, outDate:r.out_date, inDate:r.in_date,
    days:r.days, months:r.months, advance:r.advance,
    balance:r.balance, total:r.total, status:r.status,
    sheetType:r.sheet_type,
    date: r.created_at ? new Date(r.created_at).toLocaleDateString('en-IN',{day:'2-digit',month:'short',year:'numeric'}) : '',
    items:(r.sale_items||[]).map(i=>({name:i.product,qty:i.qty,price:i.price,type:i.rent_type,amount:i.amount}))
  }));
  return ok(res,{bills,count:bills.length});
}

// ── GET RETURNS ───────────────────────────────────────────────
async function getReturns(req,res) {
  const q = (req.query.q||'').trim().toLowerCase();
  let filter = 'select=*,return_items(*)&order=id.desc';
  if (q) filter += `&or=(return_bill_no.ilike.*${q}*,original_bill_no.ilike.*${q}*,customer.ilike.*${q}*,phone.ilike.*${q}*)`;
  const rows = await sb(`returns?${filter}`);
  const returns = rows.map(r=>({
    returnBillNo:r.return_bill_no, originalBillNo:r.original_bill_no,
    customer:r.customer, phone:r.phone, returnDate:r.return_date,
    actualDays:r.actual_days, actualMonths:r.actual_months,
    totalRental:r.total_rental, prevAdvance:r.prev_advance,
    additionalPay:r.additional_pay, finalBalance:r.final_balance,
    status:r.status, notes:r.notes,
    items:(r.return_items||[]).map(i=>({name:i.product,qty:i.qty}))
  }));
  return ok(res,{returns,count:returns.length});
}

// ── GET PENDING — includes both dispatch pending + return balance due ──
async function getPending(req,res) {
  const now = new Date();

  // 1. Dispatch bills with pending balance
  const salesRows = await sb(`sales?select=bill_no,customer,phone,balance,status,in_date,out_date,total,advance&balance=gt.0&status=neq.PAID&order=balance.desc`);
  const dispatchPending = salesRows.map(r=>({
    billNo:    r.bill_no,
    customer:  r.customer,
    phone:     r.phone,
    balance:   Number(r.balance)||0,
    inDate:    r.in_date,
    outDate:   r.out_date,
    total:     r.total,
    advance:   r.advance,
    type:      'DISPATCH',
    overdue:   r.in_date && new Date(r.in_date) < now
  }));

  // 2. Return bills with balance due (customer still owes)
  const retRows = await sb(`returns?select=return_bill_no,original_bill_no,customer,phone,final_balance,status,return_date&final_balance=gt.0&status=neq.FULLY PAID&order=final_balance.desc`);
  const returnPending = retRows.map(r=>({
    billNo:    r.return_bill_no,
    refBill:   r.original_bill_no,
    customer:  r.customer,
    phone:     r.phone,
    balance:   Number(r.final_balance)||0,
    inDate:    r.return_date,
    type:      'RETURN',
    overdue:   true
  }));

  // 3. Return bills where customer is owed a REFUND (negative balance)
  const refundRows = await sb(`returns?select=return_bill_no,original_bill_no,customer,phone,final_balance,status&final_balance=lt.0&order=final_balance`);
  const refunds = refundRows.map(r=>({
    billNo:    r.return_bill_no,
    refBill:   r.original_bill_no,
    customer:  r.customer,
    phone:     r.phone,
    balance:   Number(r.final_balance)||0,
    type:      'REFUND'
  }));

  const allPending = [...dispatchPending, ...returnPending];
  const total = allPending.reduce((s,x)=>s+Number(x.balance),0);
  return ok(res,{pending:allPending, refunds, total, count:allPending.length});
}

// ── GET EXCEL DATA ────────────────────────────────────────────
async function getExcel(req,res) {
  const [sales,returns,invMain,invSub] = await Promise.all([
    sb('sales?select=*,sale_items(*)&order=id.desc'),
    sb('returns?select=*,return_items(*)&order=id.desc'),
    sb('inventory_main?select=*&order=id'),
    sb('inventory_sub?select=*&order=id')
  ]);
  return ok(res,{sales,returns,invMain,invSub});
}

// ── UPLOAD XL DATA — REPLACE mode (not add) ──────────────────
async function uploadXL(data,res) {
  try {
    const {invMain, invSub} = data;

    // ── Helper: DELETE all rows then re-insert fresh ──────────
    async function replaceTable(tbl, rows) {
      if (!rows || !rows.length) return;
      // Delete ALL existing rows in table
      await sb(`${tbl}?id=gt.0`, 'DELETE');
      // Insert fresh rows one by one
      for (const row of rows) {
        if (!row.product) continue;
        await sb(tbl, 'POST', {
          product:     String(row.product).trim(),
          total_stock: Number(row.total_stock) || 0,
          in_qty:      Number(row.in_qty)      || 0,
          out_qty:     Number(row.out_qty)      || 0
        });
      }
    }

    if (invMain && invMain.length) await replaceTable('inventory_main', invMain);
    if (invSub  && invSub.length)  await replaceTable('inventory_sub',  invSub);

    return ok(res, {message: 'Inventory replaced from Excel ✅'});
  } catch(e) { return err(res, e.message); }
}

// ── SETTLE PAYMENT — handles both DISPATCH and RETURN bills ──
async function settlePayment(data,res) {
  try {
    const {billNo, payAmount, notes, type} = data;
    const paid   = Number(payAmount)||0;
    let oldBal   = 0;
    let newBal   = 0;
    let customer = '';
    let phone    = '';

    if (type === 'RETURN') {
      // Settle return balance due
      const ret = await sb(`returns?return_bill_no=eq.${encodeURIComponent(billNo)}&select=id,final_balance,customer,phone`);
      if (!ret.length) return err(res,'Return bill not found: '+billNo);
      const r = ret[0];
      customer = r.customer; phone = r.phone;
      oldBal   = Number(r.final_balance)||0;
      newBal   = Math.max(0, oldBal - paid);
      await sb(`returns?id=eq.${r.id}`, 'PATCH', {
        final_balance: newBal,
        status:        newBal<=0 ? 'FULLY PAID' : 'BALANCE DUE'
      });
    } else {
      // Settle dispatch balance
      const sale = await sb(`sales?bill_no=eq.${encodeURIComponent(billNo)}&select=id,balance,customer,phone`);
      if (!sale.length) return err(res,'Bill not found: '+billNo);
      const s  = sale[0];
      customer = s.customer; phone = s.phone;
      oldBal   = Number(s.balance)||0;
      newBal   = Math.max(0, oldBal - paid);
      await sb(`sales?id=eq.${s.id}`, 'PATCH', {
        balance: newBal,
        status:  newBal<=0 ? 'PAID' : 'PENDING'
      });
    }

    await telegram(
      `💳 *Payment Settled — ${billNo}*\n` +
      `👤 ${customer}  📞 ${phone}\n` +
      `💰 Paid: ₹${fmt(paid)} | Old Balance: ₹${fmt(oldBal)}\n` +
      (newBal<=0 ? `✅ *FULLY PAID*` : `🔴 Remaining: ₹${fmt(newBal)}`) +
      (notes ? `\n📝 ${notes}` : '')
    );
    return ok(res,{billNo, oldBalance:oldBal, paid, newBalance:newBal, status:newBal<=0?'PAID':'PENDING'});
  } catch(e) { return err(res,e.message); }
}

// ── ADD ORDER ─────────────────────────────────────────────────
async function addOrder(data,res) {
  const items = data.items||[];
  await sb('sales','POST',{
    bill_no:data.billNo, customer:data.customer, phone:data.phone,
    address:data.address||'', sheet_type:data.sheetType||'MAIN',
    out_date:data.outDate||'', in_date:data.inDate||'',
    days:Number(data.days)||0, months:Number(data.months)||0,
    advance:Number(data.advance)||0, balance:Number(data.balance)||0,
    total:Number(data.total)||0,
    status:Number(data.balance)>0?'PENDING':'PAID'
  });
  for (const it of items) {
    await sb('sale_items','POST',{
      bill_no:data.billNo, product:it.name,
      qty:Number(it.qty)||0, price:Number(it.price)||0,
      rent_type:it.type||'per day', amount:Number(it.amount)||0
    });
  }
  const inv = data.sheetType==='SUB'?'inventory_sub':'inventory_main';
  for (const it of items) {
    const rows = await sb(`${inv}?product=eq.${encodeURIComponent(it.name)}&select=id,out_qty`);
    if (rows.length) {
      await sb(`${inv}?id=eq.${rows[0].id}`,'PATCH',{out_qty:(Number(rows[0].out_qty)||0)+Number(it.qty)});
    }
  }
  await telegram(
    `✅ *New Order — ${data.billNo}*\n📅 ${new Date().toLocaleDateString('en-IN')}\n━━━━━━━━━━━━━━━━\n`+
    `👤 *Customer:* ${data.customer}\n📞 *Phone:* ${data.phone}\n📍 *Address:* ${data.address||'—'}\n━━━━━━━━━━━━━━━━\n`+
    `📦 *Items:*\n${items.map(x=>`  • ${x.name} × ${x.qty} pcs = ₹${fmt(x.amount)}`).join('\n')}\n━━━━━━━━━━━━━━━━\n`+
    `🕐 OUT: ${data.outDate||'—'}\n🕐 IN: ${data.inDate||'—'}\n⏱ ${data.days} days / ${data.months} month(s)\n━━━━━━━━━━━━━━━━\n`+
    `💰 Total: ₹${fmt(data.total)}\n✅ Advance: ₹${fmt(data.advance)}\n🔴 Balance: ₹${fmt(data.balance)}`
  );
  return ok(res,{billNo:data.billNo});
}

// ── ADD RETURN ────────────────────────────────────────────────
async function addReturn(data,res) {
  const items = data.items||[];
  const fb = Number(data.finalBalance)||0;
  await sb('returns','POST',{
    return_bill_no:data.returnBillNo, original_bill_no:data.originalBillNo,
    customer:data.customer, phone:data.phone,
    return_date:data.returnDate||new Date().toISOString(),
    actual_days:Number(data.actualDays)||0, actual_months:Number(data.actualMonths)||0,
    total_rental:Number(data.totalRental)||0, prev_advance:Number(data.prevAdvance)||0,
    additional_pay:Number(data.additionalPay)||0, final_balance:fb,
    status:fb<=0?'FULLY PAID':'BALANCE DUE', notes:data.notes||''
  });
  for (const it of items) {
    await sb('return_items','POST',{return_bill_no:data.returnBillNo,product:it.name,qty:Number(it.qty)||0});
  }
  const inv = data.sheetType==='SUB'?'inventory_sub':'inventory_main';
  for (const it of items) {
    const rows = await sb(`${inv}?product=eq.${encodeURIComponent(it.name)}&select=id,in_qty`);
    if (rows.length) {
      await sb(`${inv}?id=eq.${rows[0].id}`,'PATCH',{in_qty:(Number(rows[0].in_qty)||0)+Number(it.qty)});
    }
  }
  const sale = await sb(`sales?bill_no=eq.${encodeURIComponent(data.originalBillNo)}&select=id,balance`);
  if (sale.length) {
    const newBal = Math.max(0,(Number(sale[0].balance)||0)-(Number(data.additionalPay)||0));
    await sb(`sales?id=eq.${sale[0].id}`,'PATCH',{balance:newBal,status:newBal<=0?'PAID':'PENDING'});
  }
  await telegram(
    `📦 *Return — ${data.returnBillNo}*\n📅 ${new Date().toLocaleDateString('en-IN')}\n━━━━━━━━━━━━━━━━\n`+
    `👤 *Customer:* ${data.customer}\n📞 *Phone:* ${data.phone}\n🔗 *Ref Bill:* ${data.originalBillNo}\n━━━━━━━━━━━━━━━━\n`+
    `📦 *Items Returned:*\n${items.map(x=>`  • ${x.name} × ${x.qty} pcs`).join('\n')}\n━━━━━━━━━━━━━━━━\n`+
    `⏱ Actual: ${data.actualDays} days / ${data.actualMonths} month(s)\n`+
    `💰 Total Rental: ₹${fmt(data.totalRental)}\n✅ Prev Advance: ₹${fmt(data.prevAdvance)}\n➕ Add Payment: ₹${fmt(data.additionalPay)}\n━━━━━━━━━━━━━━━━\n`+
    (fb<0?`🟡 *REFUND TO CUSTOMER: ₹${fmt(Math.abs(fb))}*`:fb===0?`🟢 *FULLY PAID*`:`🔴 *Balance Due: ₹${fmt(fb)}*`)
  );
  return ok(res,{returnBillNo:data.returnBillNo});
}
