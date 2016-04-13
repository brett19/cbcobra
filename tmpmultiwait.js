function TmpMultiWait(num) {
  this.waitCount = 1 + num;
  this.callback = null;
}
TmpMultiWait.prototype.tick = function() {
  this.waitCount--;
  if (this.waitCount === 0) {
    if (this.callback) {
      this.callback();
    }
  }
};
TmpMultiWait.prototype.wait = function(callback) {
  this.callback = callback;
  this.tick();
};

module.exports = TmpMultiWait;
