ReplSetTest.prototype.getPath = function ReplSetTest_getPath(n) {
  if (n.host) {
    n = this.getNodeId(n);
  }
  var p = "tmp/db-" + this.name + "-" + n;
  if (!this._alldbpaths) {
    this._alldbpaths = [p];
  } else {
    this._alldbpaths.push(p);
  }
  return p;
}
