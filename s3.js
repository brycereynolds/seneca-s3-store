'use strict';

var aws = require('aws-sdk');
var error = require('eraro')({ package: 'seneca-s3-store' });
var name = 's3';

module.exports = function(options) {
  var _this = this;

  options = _this.util.deepextend({
    apiVersion: '2006-03-01',
    s3ForcePathStyle: true,
    accessKeyId: null,
    secretAccessKey: null,
    endpoint: null,
    region: null,
    bucket: null,
  }, options);

  var s3Client = new aws.S3({
    apiVersion: options.apiVersion,
    s3ForcePathStyle: options.s3ForcePathStyle,
    accessKeyId: options.accessKeyId,
    setAccessKey: options.awsSecretAccessKey,
    endpoint: options.endpoint,
    region: options.region,
  });

  var store = {
    name: name,

    save: function(args, cb) {
      var ent = args.ent;

      s3Client.upload({
        Key: ent.target,
        Body: ent.buffer,
        ContentType: ent.type,
        Bucket: options.bucket,
      }, function(err) {

        if (err) {
          return cb(error('upload-error', {
            store: name,
            error: err,
            args: args,
          }));
        }

        _this.log.debug(args.actid$, 'save/insert', ent);

        return cb(null, {
          ok: true,
          path: ent.target,
          bucket: options.bucket,
        });
      });
    },

    list: function(args, cb) {
      _this.log.error(args.actid$, 'list', 'Function not implemented');
      return cb(error('not-implemented'), null);
    },

    remove: function(args, cb) {
      _this.log.error(args.actid$, 'remove', 'Function not implemented');
      return cb(error('not-implemented'), null);
    },
  };

  var meta = _this.store.init(_this, options, store);

  _this.add({
    init: store.name,
    tag: meta.tag,
  }, function(args, cb) {

    s3Client.headObject({
      Key: '',
      Bucket: options.bucket,
    }, function(err, response) {

      if (err) {
        return cb(error('bucket-not-found', {
          bucket: options.bucket,
          store: meta.desc,
        }));
      }

      return cb();
    });

  });

  return {
    name: store.name,
    tag: meta.tag,
  };
};
