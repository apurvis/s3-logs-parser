<?php

namespace S3LogsParser;

use Aws\Credentials\Credentials;
use Aws\S3\S3Client;
use Carbon\Carbon;

class S3LogsParser
{
    /** @var \Aws\S3\S3Client|null $client */
    protected $client = null;

    /** @var array $configs */
    protected $configs = [
        'version' => 'latest',
        'region' => '',
        'access_key' => '',
        'secret_key' => '',
    ];

    /** @var string $regex https://docs.aws.amazon.com/AmazonS3/latest/dev/LogFormat.html */
    protected $regex = '/(?P<owner>\S+) (?P<bucket>\S+) (?P<time>\[[^]]*\]) (?P<ip>\S+) '.
        '(?P<requester>\S+) (?P<reqid>\S+) (?P<operation>\S+) (?P<key>\S+) (?P<request>"[^"]*") '.
        '(?P<status>\S+) (?P<error>\S+) (?P<bytes>\S+) (?P<size>\S+) (?P<totaltime>\S+) '.
        '(?P<turnaround>\S+) (?P<referrer>"[^"]*") (?P<useragent>"[^"]*") (?P<version>\S)/';

    /**
     * S3LogsParser constructor.
     *
     * @param array         $configs
     * @param S3Client|null $client
     */
    public function __construct(array $configs = [], S3Client $client = null)
    {
        $this->setConfigs($configs);
        $this->client = $client;
    }

    /**
     * @param string $key
     *
     * @return string
     */
    public function getConfig(string $key) : string
    {
        return isset($this->configs[$key]) ? $this->configs[$key] : '';
    }

    /**
     * @param array $configs
     *
     * @return array
     */
    public function setConfigs(array $configs = []) : array
    {
        foreach ($configs as $key => $value) {
            if (is_string($key) && is_string($value)) {
                if (mb_strlen($key) && mb_strlen($value)) {
                    if (array_key_exists($key, $this->configs)) {
                        $this->configs[$key] = $value;
                    }
                }
            }
        }

        return $this->configs;
    }

    /**
     * @param string $bucketName
     * @param string $bucketPrefix
     * @param string $date
     *
     * @return string|false
     */
    public function getStats(string $bucketName, string $bucketPrefix, string $date)
    {
        $logLines = [];

        if array_key_exists('logs_location', $this->configs) {
          $logLines = $this->loadLogsFromLocalDir($this->getConfig('logs_location'));
        } else {
          $logLines = $this->loadLogsFromS3($bucketName, $bucketPrefix);
        }

        return json_encode([
            'success' => true,
            'statistics' => [
                // 'bucket' => $listObjectsParams['Bucket'],
                // 'prefix' => $listObjectsParams['Prefix'],
                'data' => $this->extractStatistics($logLines),
            ],
        ]);
    }

    /**
     * @param string $parsedLogs
     *
     * @return hash
     */
    public function loadLogsFromS3(string $bucketName, string $bucketPrefix) : array
    {
      $logLines = [];

      $listObjectsParams = [
          'Bucket' => $bucketName,
          'Prefix' => sprintf('%s%s', $bucketPrefix, Carbon::parse($date)->format('Y-m-d')),
      ];

      $results = $this->getClient()->getPaginator('ListObjects', $listObjectsParams);

      foreach ($results as $result) {
          if (isset($result['Contents'])) {
              foreach ($result['Contents'] as $object) {
                  $logLines += $this->parseS3Object($bucketName, $object['Key']);
              }
          }
      }

      return $logLines;
    }

    /**
     * @param string $logDir
     *
     * @return hash
     */
    public function loadLogsFromLocalDir(string $logDir) : array
    {
      $logLines = [];

      foreach (new DirectoryIterator($this->getConfig('logs_location')) as $file) {
          if($file->isDot()) continue;
          echo $file->getFilename() . "<br>\n";
          $logLines += $this->processLogsStringToArray(file_get_contents($file, true));
      }

      return $logLines;
    }

    /**
     * @param string $parsedLogs
     *
     * @return hash
     */
    public function extractStatistics(array $parsedLogs)
    {
      foreach ($parsedLogs as $item) {
          if (isset($item['key']) && mb_strlen($item['key'])) {
              if (!isset($statistics[$item['key']]['downloads'])) {
                  $statistics[$item['key']]['downloads'] = 0;
              }

              if (!isset($statistics[$item['key']]['bandwidth'])) {
                  $statistics[$item['key']]['bandwidth'] = 0;
              }

              $statistics[$item['key']]['downloads'] += 1;

              if (isset($item['bytes'])) {
                  $statistics[$item['key']]['bandwidth'] += (int) $item['bytes'];
              }
          }
      }
    }

    /**
     * @param string $bucketName
     * @param string $key
     *
     * @return array
     */
    public function parseS3Object(string $bucketName, string $key) : array
    {
        $output = [];

        $file = $this->getClient()->getObject([
            'Bucket' => $bucketName,
            'Key' => $key,
        ]);

        return $this->processLogsStringToArray((string) $file['Body']);
    }

    /**
     * @param string $logsString
     *
     * @return array
     */
    public function processLogsStringToArray(string $logsString) : array
    {
        $rows = explode("\n", (string) $logsString);

        foreach ($rows as $row) {
            preg_match($this->regex, $row, $matches);

            if (isset($matches['operation']) && $matches['operation'] == 'REST.GET.OBJECT') {
                $output[] = $matches;
            }
        }

        return $output;
    }

    /**
     * @return S3Client
     */
    public function getClient() : S3Client
    {
        if (is_null($this->client)) {
            $this->client = new S3Client([
                'version' => $this->getConfig('version'),
                'region' => $this->getConfig('region'),
                'credentials' => new Credentials(
                    $this->getConfig('access_key'),
                    $this->getConfig('secret_key')
                ),
            ]);
        }

        return $this->client;
    }
}
