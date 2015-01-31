#!/usr/bin/env node

var path = require('path');
var http = require('http');
var Transform = require('stream').Transform;
var util = require('util');

var bunyan = require('bunyan');
var lstream = require('lstream');
var manta = require('manta');
var vasync = require('vasync');

var LOG = bunyan.createLogger({
  name: path.basename(process.argv[1]),
  level: (process.env.LOG_LEVEL || 'info'),
  stream: process.stderr,
});

var HOSTNAME="http://ddebs.ubuntu.com";
var RELEASE='trusty';
var COMPONENT='main';
var ARCH='i386';

var MANTA_BASE='/NodeCore/public/ubuntu/';
var MANTA_PARALLEL=1;

function PackageParser() {
  Transform.call(this, {
    decodeStrings: false,
    objectMode: true,
    highWaterMark: 1,
  });

  this.pp_cur = undefined;
}
util.inherits(PackageParser, Transform);

PackageParser.prototype._transform = function ppTransform(chunk, enc, done) {
  var m = chunk.match(/^(\w+): (.*)$/);

  if (m) {
    if (m[1] === 'Package' && this.pp_cur) {
      if (this.pp_cur.Package &&
          this.pp_cur.MD5sum &&
          this.pp_cur.Filename) {
        this.push(this.pp_cur);
        this.pp_cur = {};
      } else {
        LOG.error('Incomplete package:', this.pp_cur);
      }
    }

    if (!this.pp_cur) this.pp_cur = {};

    this.pp_cur[m[1]] = m[2];
  }

  done();
};

PackageParser.prototype._flush = function ppFlush(done) {
  if (this.pp_cur && this.pp_cur.Filename)
    this.push(this.pp_cur);

  done();
};

var mantaClient = manta.createBinClient({
  log: LOG,
});

function syncPackage(pkg, cb) {
  var mantaDest = util.format('%s/%s', MANTA_BASE, pkg.Filename);
  var debPath = util.format('%s/%s', HOSTNAME, pkg.Filename);

  var mantaPutOpts = {
    mkdirs: true,
    size: +pkg.Size,
    //md5: pkg.MD5sum,
  };

  LOG.info('syncing', debPath, 'to', mantaDest, mantaPutOpts);

  var pkgReq = http.get(debPath);

  pkgReq.on('response', function pkgGet(res) {
    mantaClient.put(mantaDest, res, mantaPutOpts, function afterPut(err, res) {
      if (err) {
        console.error(err);
        process.exit(1);
      }

      LOG.info('successfully put', mantaDest);
      cb();
    });
  });
}

function MantaSync() {
  Transform.call(this, {
    objectMode: true,
    highWaterMark: MANTA_PARALLEL,
  });

  this.ms_queue = vasync.queue(syncPackage, MANTA_PARALLEL);
}
util.inherits(MantaSync, Transform);

MantaSync.prototype._transform = function msTransform(chunk, enc, done) {
  var checkPath = util.format('%s/%s', MANTA_BASE, chunk.Filename);
  var self = this;
  mantaClient.info(checkPath, function infoCheck(info_err, info_meta) {
    if (info_err && info_err.statusCode === 404) {
      self.ms_queue.push(chunk);
      return done();
    } else if (info_err) {
      console.error(info_err);
      process.exit(2);
    }

    var md5 = new Buffer(info_meta.md5, 'base64');
    md5 = md5.toString('hex');

    if (info_meta && md5 !== chunk.MD5sum) {
      LOG.info('md5 mismatch', info_meta, chunk);
      self.ms_queue.push(chunk);
    }
    done();
  });
};


// XXX Main

var packagePath = util.format('%s/dists/%s/%s/binary-%s/Packages',
                              HOSTNAME, RELEASE, COMPONENT, ARCH);

LOG.info('getting', packagePath);

var req = http.get(packagePath);

req.on('response', function(res) {
  var mantaDest = util.format('%s/dists/%s/%s/binary-%s/Packages',
                              MANTA_BASE, RELEASE, COMPONENT, ARCH);

  var dest = mantaClient.createWriteStream(mantaDest, { mkdirs: true });

  dest.on('finish', function afterPut(err, res) {
    if (err) {
      console.error('failed to put Package file');
      console.error(err);
      process.exit(2);
    }
    LOG.info('put Package file');
  });

  res.pipe(dest);

  res.pipe(new lstream()).pipe(new PackageParser())
    .pipe(new MantaSync())
    .resume();
});
