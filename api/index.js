// ================================================================
// GARUDA CONSTRUCTION SERVICE — Vercel API v3
// Fixed: SUB inventory sync using JS-side name matching
// ================================================================

const SUPABASE_URL    = process.env.SUPABASE_URL;
const SUPABASE_SECRET = process.env.SUPABASE_SECRET;
const TG_TOKEN        = process.env.TG_TOKEN;
const TG_CHAT         = process.env.TG_CHAT;

async function sb(path, method='GET', body=null) {
  const opts = {
    method,
    headers: {
      'apikey':        SUPABASE_SECRET,
      'Authorization': `Bearer ${SUPABASE_SECRET}`,
      'Content-Type':  'application/json',
      'Prefer': method==='POST'?'return=representation':method==='DELETE'?'return=minimal':''
    }
  };
  if (body) opts.body = JSON.stringify(body);
  const r = await fetch(`${SUPABASE_URL}/rest/v1/${path}`, opts);
  if (!r.ok) { const e = await r.text(); throw new Error(`DB ${method} /${path}: ${e}`); }
  return r.status===204 ? [] : r.json();
}

// KEY FIX: fetch ALL rows and match name in JS — no URL encoding issues with / and -
async function findInvRow(tbl, productName) {
  const rows = await sb(`${tbl}?select=id,product,in_qty,out_qty,total_stock&order=id`);
  const pLow = (productName||'').trim().toLowerCase();
  return rows.find(r => (r.product||'').trim().toLowerCase() === pLow) || null;
}

async function updateInv(productName, group, field, delta) {
  const tables = group==='SUB'
    ? ['inventory_sub','inventory_main']
    : ['inventory_main','inventory_sub'];
  for (const tbl of tables) {
    const row = await findInvRow(tbl, productName);
    if (row) {
      const newVal = Math.max(0, (Number(row[field])||0) + delta);
      await sb(`${tbl}?id=eq.${row.id}`, 'PATCH', {[field]: newVal});
      return;
    }
  }
  // Auto-create if not found
  const tbl2 = group==='SUB' ? 'inventory_sub' : 'inventory_main';
  const nr   = {product:productName.trim(), total_stock:0, in_qty:0, out_qty:0};
  nr[field]  = Math.max(0, delta);
  await sb(tbl2, 'POST', nr);
}

async function telegram(msg) {
  try {
    await fetch(`https://api.telegram.org/bot${TG_TOKEN}/sendMessage`, {
      method:'POST', headers:{'Content-Type':'application/json'},
      body: JSON.stringify({chat_id:TG_CHAT, text:msg, parse_mode:'Markdown'})
    });
  } catch(e) {}
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
      if (action==='ping')        return ok(res,{message:'Garuda API v3 ✅'});
      if (action==='getStock')    return await getStock(req,res);
      if (action==='getBills')    return await getBills(req,res);
      if (action==='getBillById') return await getBillById(req,res);
      if (action==='getReturns')  return await getReturns(req,res);
      if (action==='getPending')  return await getPending(req,res);
      if (action==='getExcel')    return await getExcel(req,res);
      return ok(res,{message:'Garuda API v3 ✅'});
    }
    if (req.method==='POST') {
      const d = req.body;
      if (d.action==='addOrder')       return await addOrder(d,res);
      if (d.action==='addReturn')      return await addReturn(d,res);
      if (d.action==='settlePayment')  return await settlePayment(d,res);
      if (d.action==='uploadXL')       return await uploadXL(d,res);
      if (d.action==='restoreMissing') return await restoreMissing(d,res);
      return err(res,'Unknown: '+d.action);
    }
    return err(res,'Method not allowed');
  } catch(e) { console.error(e); return err(res,e.message); }
}

async function getStock(req,res) {
  const tbl  = req.query.sheet==='SUB' ? 'inventory_sub' : 'inventory_main';
  const rows = await sb(`${tbl}?select=*&order=id`);
  return ok(res,{stock: rows.map(r=>({
    name:      r.product,
    total:     Number(r.total_stock)||0,
    in_qty:    Number(r.in_qty)||0,
    out_qty:   Number(r.out_qty)||0,
    available: Math.max(0,(Number(r.total_stock)||0)-(Number(r.out_qty)||0)+(Number(r.in_qty)||0)),
    out:       Math.max(0,(Number(r.out_qty)||0)-(Number(r.in_qty)||0))
  }))});
}

async function getBills(req,res) {
  const q = (req.query.q||'').trim();
  let f = 'select=*,sale_items(*)&order=id.desc';
  if (q) f += `&or=(bill_no.ilike.*${q}*,customer.ilike.*${q}*,phone.ilike.*${q}*)`;
  const rows = await sb(`sales?${f}`);
  return ok(res,{bills: rows.map(mapBill), count:rows.length});
}

async function getBillById(req,res) {
  const id = (req.query.id||'').trim();
  if (!id) return err(res,'ID required');
  const rows = await sb(`sales?select=*,sale_items(*)&or=(bill_no.ilike.*${id}*,customer.ilike.*${id}*,phone.ilike.*${id}*)&order=id.desc`);
  return ok(res,{bills: rows.map(mapBill), count:rows.length});
}

function mapBill(r) {
  return {
    billNo:r.bill_no, customer:r.customer, phone:r.phone, address:r.address,
    outDate:r.out_date, inDate:r.in_date, days:r.days, months:r.months,
    advance:r.advance, balance:r.balance, total:r.total, status:r.status,
    sheetType:r.sheet_type,
    date:r.created_at?new Date(r.created_at).toLocaleDateString('en-IN',{day:'2-digit',month:'short',year:'numeric'}):'',
    items:(r.sale_items||[]).map(i=>({name:i.product,qty:i.qty,price:i.price,type:i.rent_type,amount:i.amount,group:i.group_type||'MAIN'}))
  };
}

async function getReturns(req,res) {
  const q = (req.query.q||'').trim();
  let f = 'select=*,return_items(*)&order=id.desc';
  if (q) f += `&or=(return_bill_no.ilike.*${q}*,original_bill_no.ilike.*${q}*,customer.ilike.*${q}*,phone.ilike.*${q}*)`;
  const rows = await sb(`returns?${f}`);

  const returns = await Promise.all(rows.map(async r => {
    const retItems = (r.return_items||[]).map(i=>({name:i.product,qty:Number(i.qty)||0,group:i.group_type||'MAIN'}));
    let missingItems = [];
    try {
      const orig = await sb(`sales?bill_no=eq.${r.original_bill_no}&select=sale_items(*)`);
      if (orig.length && orig[0].sale_items) {
        orig[0].sale_items.forEach(si=>{
          const ret    = retItems.find(ri=>ri.name.trim().toLowerCase()===si.product.trim().toLowerCase());
          const retQty = ret?ret.qty:0;
          const miss   = Math.max(0,(Number(si.qty)||0)-retQty);
          if (miss>0) missingItems.push({name:si.product,dispatched:Number(si.qty)||0,returned:retQty,missing:miss,price:Number(si.price)||0,group:si.group_type||'MAIN'});
        });
      }
    } catch(e) {}
    return {
      returnBillNo:r.return_bill_no, originalBillNo:r.original_bill_no,
      customer:r.customer, phone:r.phone, returnDate:r.return_date,
      actualDays:r.actual_days, actualMonths:r.actual_months,
      totalRental:r.total_rental, prevAdvance:r.prev_advance,
      additionalPay:r.additional_pay, finalBalance:r.final_balance,
      status:r.status, notes:r.notes, items:retItems, missingItems
    };
  }));
  return ok(res,{returns,count:returns.length});
}

async function getPending(req,res) {
  const now = new Date();
  const [sRows,rRows,refRows] = await Promise.all([
    sb(`sales?select=bill_no,customer,phone,balance,status,in_date,out_date,total,advance&balance=gt.0&status=neq.PAID&order=balance.desc`),
    sb(`returns?select=return_bill_no,original_bill_no,customer,phone,final_balance,status,return_date&final_balance=gt.0&order=final_balance.desc`),
    sb(`returns?select=return_bill_no,original_bill_no,customer,phone,final_balance,status&final_balance=lt.0&order=final_balance`)
  ]);
  const disp = sRows.map(r=>({billNo:r.bill_no,customer:r.customer,phone:r.phone,balance:Number(r.balance)||0,inDate:r.in_date,outDate:r.out_date,total:r.total,advance:r.advance,type:'DISPATCH',overdue:r.in_date&&new Date(r.in_date)<now}));
  const ret  = rRows.map(r=>({billNo:r.return_bill_no,refBill:r.original_bill_no,customer:r.customer,phone:r.phone,balance:Number(r.final_balance)||0,inDate:r.return_date,type:'RETURN',overdue:true}));
  const ref  = refRows.map(r=>({billNo:r.return_bill_no,refBill:r.original_bill_no,customer:r.customer,phone:r.phone,balance:Number(r.final_balance)||0,type:'REFUND'}));
  const all  = [...disp,...ret];
  return ok(res,{pending:all,refunds:ref,total:all.reduce((s,x)=>s+x.balance,0),count:all.length});
}

async function getExcel(req,res) {
  const [sRaw,rRaw,invM,invS] = await Promise.all([
    sb('sales?select=*,sale_items(*)&order=id.desc'),
    sb('returns?select=*,return_items(*)&order=id.desc'),
    sb('inventory_main?select=*&order=id'),
    sb('inventory_sub?select=*&order=id')
  ]);
  return ok(res,{
    sales:   sRaw.map(s=>({...s,sale_items:s.sale_items||[],items_summary:(s.sale_items||[]).map(i=>`${i.product}×${i.qty}`).join(', ')})),
    returns: rRaw.map(r=>({...r,return_items:r.return_items||[],items_summary:(r.return_items||[]).map(i=>`${i.product}×${i.qty}`).join(', ')})),
    invMain: invM,
    invSub:  invS
  });
}

async function addOrder(data,res) {
  const items = data.items||[];
  await sb('sales','POST',{
    bill_no:data.billNo, customer:data.customer, phone:data.phone, address:data.address||'',
    sheet_type:data.sheetType||'MAIN', out_date:data.outDate||'', in_date:data.inDate||'',
    days:Number(data.days)||0, months:Number(data.months)||0,
    advance:Number(data.advance)||0, balance:Number(data.balance)||0, total:Number(data.total)||0,
    status:Number(data.balance)>0?'PENDING':'PAID'
  });
  for (const it of items) {
    await sb('sale_items','POST',{
      bill_no:data.billNo, product:it.name, qty:Number(it.qty)||0,
      price:Number(it.price)||0, rent_type:it.type||'per day',
      amount:Number(it.amount)||0, group_type:it.group||'MAIN'
    });
  }
  for (const it of items) {
    await updateInv(it.name, it.group||'MAIN', 'out_qty', Number(it.qty)||0);
  }
  await telegram(`✅ *New Order — ${data.billNo}*\n👤 ${data.customer} | 📞 ${data.phone}\n📦 ${items.map(x=>`${x.name}×${x.qty}`).join(', ')}\n💰 Total: ₹${fmt(data.total)} | Balance: ₹${fmt(data.balance)}`);
  return ok(res,{billNo:data.billNo});
}

async function addReturn(data,res) {
  const items = data.items||[];
  const fb    = Number(data.finalBalance)||0;
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
    await sb('return_items','POST',{return_bill_no:data.returnBillNo,product:it.name,qty:Number(it.qty)||0,group_type:it.group||'MAIN'});
  }
  for (const it of items) {
    await updateInv(it.name, it.group||'MAIN', 'in_qty', Number(it.qty)||0);
  }
  try {
    const sr = await sb(`sales?bill_no=eq.${data.originalBillNo}&select=id,balance`);
    if (sr.length) {
      const nb = Math.max(0,(Number(sr[0].balance)||0)-(Number(data.additionalPay)||0));
      await sb(`sales?id=eq.${sr[0].id}`,'PATCH',{balance:nb,status:nb<=0?'PAID':'PENDING'});
    }
  } catch(e) {}
  await telegram(`📦 *Return — ${data.returnBillNo}*\n👤 ${data.customer} | Ref: ${data.originalBillNo}\n📦 ${items.map(x=>`${x.name}×${x.qty}`).join(', ')}\n${fb<=0?'✅ FULLY PAID':'🔴 Balance: ₹'+fmt(fb)}`);
  return ok(res,{returnBillNo:data.returnBillNo});
}

async function settlePayment(data,res) {
  try {
    const {billNo,payAmount,notes,type} = data;
    const paid = Number(payAmount)||0;
    let oldBal=0,newBal=0,customer='',phone='';
    if (type==='RETURN') {
      const rows = await sb(`returns?return_bill_no=eq.${billNo}&select=id,final_balance,customer,phone`);
      if (!rows.length) return err(res,'Return bill not found: '+billNo);
      customer=rows[0].customer; phone=rows[0].phone;
      oldBal=Number(rows[0].final_balance)||0; newBal=Math.max(0,oldBal-paid);
      await sb(`returns?id=eq.${rows[0].id}`,'PATCH',{final_balance:newBal,status:newBal<=0?'FULLY PAID':'BALANCE DUE'});
    } else {
      const rows = await sb(`sales?bill_no=eq.${billNo}&select=id,balance,customer,phone`);
      if (!rows.length) return err(res,'Bill not found: '+billNo);
      customer=rows[0].customer; phone=rows[0].phone;
      oldBal=Number(rows[0].balance)||0; newBal=Math.max(0,oldBal-paid);
      await sb(`sales?id=eq.${rows[0].id}`,'PATCH',{balance:newBal,status:newBal<=0?'PAID':'PENDING'});
    }
    await telegram(`💳 *Settled — ${billNo}*\n👤 ${customer} | Paid: ₹${fmt(paid)}\n${newBal<=0?'✅ FULLY PAID':'🔴 Remaining: ₹'+fmt(newBal)}`);
    return ok(res,{billNo,oldBalance:oldBal,paid,newBalance:newBal,status:newBal<=0?'PAID':'PENDING'});
  } catch(e) { return err(res,e.message); }
}

async function restoreMissing(data,res) {
  try {
    const {returnBillNo,items,notes} = data;

    // 1. Update inventory IN qty for each restored item
    for (const item of (items||[])) {
      await updateInv(item.name, item.group||'MAIN', 'in_qty', Number(item.qty)||0);
    }

    // 2. Update return_items qty so missingItems no longer shows them
    //    Find each return_item and increase its qty by the restored amount
    if (returnBillNo) {
      // Fetch all return_items for this bill, then match in JS (avoids URL encoding issues)
      const allRetItems = await sb(`return_items?return_bill_no=eq.${returnBillNo}&select=id,product,qty`);
      for (const item of (items||[])) {
        const pLow = item.name.trim().toLowerCase();
        const found = allRetItems.find(ri => (ri.product||'').trim().toLowerCase() === pLow);
        if (found) {
          const newQty = (Number(found.qty)||0) + Number(item.qty);
          await sb(`return_items?id=eq.${found.id}`, 'PATCH', {qty: newQty});
        } else {
          await sb('return_items','POST',{return_bill_no:returnBillNo, product:item.name, qty:Number(item.qty)||0, group_type:item.group||'MAIN'});
        }
      }

      // 3. Update return record notes + status
      const rows = await sb(`returns?return_bill_no=eq.${returnBillNo}&select=id,notes,final_balance`);
      if (rows.length) {
        const note = `[Restored: ${items.map(i=>i.name+'×'+i.qty).join(', ')}${notes?' — '+notes:''}]`;
        // If final_balance is also 0, mark fully returned
        const newStatus = Number(rows[0].final_balance)<=0 ? 'FULLY RETURNED' : 'BALANCE DUE';
        await sb(`returns?id=eq.${rows[0].id}`,'PATCH',{
          notes: [(rows[0].notes||''), note].filter(Boolean).join(' '),
          status: newStatus
        });
      }
    }

    await telegram(`✅ *Restored — ${returnBillNo||''}*\n📦 ${items.map(i=>i.name+'×'+i.qty).join(', ')}`);
    return ok(res,{message:'Restored ✅'});
  } catch(e) { return err(res,e.message); }
}

async function uploadXL(data,res) {
  try {
    const {invMain,invSub} = data;
    async function syncTable(tbl, rows) {
      if (!rows||!rows.length) return;
      const existing = await sb(`${tbl}?select=id,product&order=id`);
      for (const row of rows) {
        if (!row.product) continue;
        const pLow = row.product.trim().toLowerCase();
        const found = existing.find(e=>(e.product||'').trim().toLowerCase()===pLow);
        const payload = {product:row.product.trim(),total_stock:Number(row.total_stock)||0,in_qty:Number(row.in_qty)||0,out_qty:Number(row.out_qty)||0};
        if (found) await sb(`${tbl}?id=eq.${found.id}`,'PATCH',payload);
        else        await sb(tbl,'POST',payload);
      }
    }
    if (invMain&&invMain.length) await syncTable('inventory_main',invMain);
    if (invSub&&invSub.length)   await syncTable('inventory_sub',invSub);
    return ok(res,{message:'Inventory synced ✅'});
  } catch(e) { return err(res,e.message); }
}
