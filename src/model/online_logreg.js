const { fnv1a } = require('../utils/hash');

class OnlineLogReg {
  constructor(opts = {}) {
    this.dim = opts.dim ?? (1 << 17);
    this.lr = opts.lr ?? 0.05;
    this.l2 = opts.l2 ?? 1e-6;
    this.bias = opts.bias ?? 1.0;
    this.w = new Float64Array(this.dim);
  }
  _sigmoid(z){ if (z < -35) return 0; if (z > 35) return 1; return 1/(1+Math.exp(-z)); }
  _dot(feat){ let s=0; for (const [i,v] of feat) s += this.w[i]*v; return s; }
  predictProba(kv){ const f=this._featurize(kv); const z=this._dot(f); return this._sigmoid(z); }
  update(kv,y){ const f=this._featurize(kv); const z=this._dot(f); const p=this._sigmoid(z); const g=p-y;
    for (const [i,v] of f){ const grad = g*v + this.l2*this.w[i]; this.w[i] -= this.lr*grad; } }
  _featurize(kv){
    const arr=[[0,this.bias]];
    for (const k in kv){
      const val = kv[k];
      if (val===null||val===undefined) continue;
      if (typeof val === 'number'){
        const idx = (fnv1a('n:'+k) % ((this.dim-1))) + 1;
        arr.push([idx, val]);
      } else {
        const idx = (fnv1a('c:'+k+'='+String(val)) % ((this.dim-1))) + 1;
        arr.push([idx, 1]);
      }
    }
    return arr;
  }
  toJSON(){ return {dim:this.dim,lr:this.lr,l2:this.l2,bias:this.bias,w:Array.from(this.w)}; }
  static fromJSON(obj){ const m=new OnlineLogReg({dim:obj.dim,lr:obj.lr,l2:obj.l2,bias:obj.bias}); m.w=Float64Array.from(obj.w); return m; }
}
module.exports = { OnlineLogReg };
