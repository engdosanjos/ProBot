// src/learning/online_model.js
const sigmoid = (z) => 1 / (1 + Math.exp(-z));

class OnlineLogistic {
  constructor({ name, dims, lr = 0.03, l2 = 1e-6, initWeights = null }) {
    this.name = name;
    this.dims = dims;
    this.lr = lr;
    this.l2 = l2;
    this.w = initWeights && initWeights.length === dims ? initWeights.slice() : Array(dims).fill(0);
    this.trained = 0;
  }

  predictProb(x) {
    let z = 0;
    for (let i = 0; i < this.dims; i++) z += this.w[i] * x[i];
    return sigmoid(z);
  }

  update(x, y, sampleWeight = 1.0) {
    const p = this.predictProb(x);
    const err = (p - y) * sampleWeight;
    for (let i = 0; i < this.dims; i++) {
      const g = err * x[i] + this.l2 * this.w[i];
      this.w[i] -= this.lr * g;
    }
    this.trained += 1;
    return { p, err };
  }

  toJSON() {
    return {
      name: this.name,
      dims: this.dims,
      lr: this.lr,
      l2: this.l2,
      weights: this.w,
      trained: this.trained,
      saved_at: Date.now()
    };
  }

  static fromJSON(obj) {
    return new OnlineLogistic({
      name: obj.name,
      dims: obj.dims,
      lr: obj.lr,
      l2: obj.l2,
      initWeights: obj.weights
    });
  }
}

module.exports = { OnlineLogistic };
