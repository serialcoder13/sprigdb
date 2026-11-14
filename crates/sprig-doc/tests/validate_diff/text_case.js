// $text with $caseSensitive / $diacriticSensitive: run with
// mongosh against MongoDB 8.0 and against a SprigDB gateway; the outputs must be equal
// (9 of 9 on 12 Nov 2026). Uses and drops the database m3_text.
const d = db.getSiblingDB("m3_text");
d.dropDatabase();
d.t.insertMany([{_id:1, s:"Le Café Running fast"}, {_id:2, s:"le cafe running slow"}, {_id:3, s:"CAFÉ RUN"}, {_id:4, s:"other words"}, {_id:5, s:"Running café"}]);
d.t.createIndex({s:"text"});
sleep(1500);
const out = [];
for (const q of [
  {$search:"Café"}, {$search:"Café", $caseSensitive:true}, {$search:"café", $diacriticSensitive:true}, {$search:"cafe", $diacriticSensitive:true},
  {$search:"Running", $caseSensitive:true}, {$search:"\"Le Café\"", $caseSensitive:true}, {$search:"run", $caseSensitive:true},
  {$search:"CAFÉ", $caseSensitive:true, $diacriticSensitive:true}, {$search:"Run", $caseSensitive:true}]) {
  let ids; try { ids = d.t.find({$text: q}).sort({_id:1}).toArray().map(x=>x._id); } catch(e) { ids = "ERR " + e.code; }
  out.push(JSON.stringify(q) + " => " + JSON.stringify(ids));
}
print(out.join("\n"));
d.dropDatabase();
