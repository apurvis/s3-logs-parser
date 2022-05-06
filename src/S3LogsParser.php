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
        'logs_location' => '',
        'exclude_lines_matching' => null,
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
    public function getStats($bucketName = null, $bucketPrefix = null, $date = null) : array
    {
        $logLines = [];

        if (array_key_exists('logs_location', $this->configs)) {
          $logLines = $this->loadLogsFromLocalDir($this->getConfig('logs_location'));
        } else {
          $logLines = $this->loadLogsFromS3($bucketName, $bucketPrefix);
        }

        return $this->extractStatistics($logLines);
    }

    public function getStatsAsJSON($bucketName = null, $bucketPrefix = null, $date = null) : string
    {
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
    public function loadLogsFromS3(string $bucketName, string $bucketPrefix, $date) : array
    {
        $listObjectsParams = [
            'Bucket' => $bucketName,
            'Prefix' => $bucketPrefix + (is_null($date) ? '' : Carbon::parse($date)->format('Y-m-d')),
        ];

        $results = $this->getClient()->getPaginator('ListObjects', $listObjectsParams);
        $logLines = [];

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
      $total_operations = [];
      print $logDir;

      foreach (new \DirectoryIterator($logDir) as $file) {
          if ($file->isFile()) {
              // print 'Processing file: ' . $file->getFilename() . "...\n";
              $fileContents = file_get_contents($file->getPathname(), true);
              $processedLogs = $this->processLogsStringToArray($fileContents);
              $logLines = array_merge($logLines, $processedLogs['output']);

              foreach ($processedLogs['operations'] as $operation_name => $count) {
                  if (!array_key_exists($operation_name, $total_operations)) {
                      $total_operations[$operation_name] = 0;
                  }

                  $total_operations[$operation_name] += (int) $count;
              }
          }
      }

      print "\n\n****TOTAL OPERATIONS****\n";
      var_dump($total_operations);
      return $logLines;
    }

    /**
     * @param string $parsedLogs
     *
     * @return hash
     */
    public function extractStatistics(array $parsedLogs)
    {
        $statistics = [];

        foreach ($parsedLogs as $item) {
            if (isset($item['key']) && mb_strlen($item['key'])) {
                if (!isset($statistics[$item['key']]['downloads'])) {
                    $statistics[$item['key']]['downloads'] = 0;
                }

                if (!isset($statistics[$item['key']]['bandwidth'])) {
                    $statistics[$item['key']]['bandwidth'] = 0;
                }

                if (!isset($statistics[$item['key']]['dates'])) {
                    $statistics[$item['key']]['dates'] = [];
                }

                $statistics[$item['key']]['downloads'] += 1;
                $date = $this->parseLogDateString($item['time']);
                array_push($statistics[$item['key']]['dates'], $date);

                if (isset($item['bytes'])) {
                    // print "Downloading ".$item['bytes']." from ".$item['key']."\n";
                    // print "\n\n".print_r($item)."\n\n";
                    $statistics[$item['key']]['bandwidth'] += (int) $item['bytes'];
                }
            }
        }

        return $statistics;
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
        $output = [];
        $operations = [];

        foreach ($rows as $row) {
            $exclude_lines_matching = $this->getConfig('exclude_lines_matching');

            // Skip rows containing exclusion string
            if (!is_null($exclude_lines_matching) && str_contains($row, $exclude_lines_matching)) {
              continue;
            }

            preg_match($this->regex, $row, $matches);
            // var_dump($matches);
            if (array_key_exists('operation', $matches)) {
                $operation = $matches['operation'];

                if (!array_key_exists($operation, $operations)) {
                    $operations[$operation] = 0;
                }

                $operations[$matches['operation']] += 1;
            }

            if (isset($matches['operation']) && $matches['operation'] == 'REST.GET.OBJECT') {
                $output[] = $matches;
            }
        }

        return [
            'operations' => $operations,
            'output' => $output
        ];
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

    /**
     * @param string $dateString
     *
     * @return date
     */
    private function parseLogDateString(string $dateString)
    {
        $dateString = explode(' ', $dateString)[0];
        $dateString = ltrim($dateString, '[');
        $dateString = explode(':', $dateString)[0];
        return Carbon::createFromFormat('d/M/Y', $dateString)->format('Y-m-d');
    }
}
